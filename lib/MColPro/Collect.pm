package MColPro::Collect;

=head1 NAME

Collect - MColPro Data collector

=cut

use strict;
use warnings;

use Carp;
use threads;
use threads::shared;
use Thread::Semaphore;
use Thread::Queue;
use Time::HiRes qw( time sleep alarm stat );

use MColPro::Util::Logger;
use MColPro::Util::Plugin;
use MColPro::SqlBase;
use MColPro::Record;
use MColPro::Exclude;
use MColPro::Collect::Conf;
use MColPro::Collect::Batch;

sub new
{
    my ( $class, %param ) = @_;
    my %self;

    confess "invaild conf" 
        unless $param{colconf} && $param{config};

    $self{conf} = MColPro::Collect::Conf->new( $param{colconf} );
    $self{type} = $self{conf}{type};

    ## db
    $self{config} = MColPro::SqlBase::conf_check( $param{config} );

    $self{batch} = MColPro::Collect::Batch::batch
    ( 
        $self{conf}{target},
        $self{conf}{thread}
    );

    $self{plugin} = MColPro::Util::Plugin->new( $self{conf}{plugin} );

    $self{log} = MColPro::Util::Logger->new( \*STDERR );

    bless \%self, ref $class || $class;
}

sub run
{
    my $self = shift;

    my $interval = $self->{conf}{interval};
    my $timeout = $self->{conf}{timeout};
    my %thread;
    my $finishq = Thread::Queue->new();

    my $log = $self->{log};
    my $logsub = sub { $log->say( @_ ) };

    for my $batch ( @{ $self->{batch} } )
    {
        my $sem = Thread::Semaphore->new(0); 

        my $thread = threads::async
        {
            local $SIG{KILL} = sub{ threads->exit(); };
            my %cache;
            while(1)
            {

                while(1)
                {
                    last if $sem->down_nb();
                    sleep 0.5;
                }

                my $result = [];
                eval
                {
                    local $SIG{ALRM} = sub{ die "timeout" };
                    $result = $self->{plugin}
                    ( 
                        batch => $batch, 
                        log => $logsub, 
                        type => $self->{type},
                        cache => \%cache 
                    );
                };

                push @$result, +{
                    type => "Collect",
                    cluster => $self->{type},
                    node => threads->tid(),
                    detail => $@,
                    label => "threads error",
                    level => -1, 
                } if $@;

                $result = undef unless ( @$result );

                $log->say( "Collect: thread erorr $@" ) if $@;

                $finishq->enqueue( threads->tid(), $result );
            }
        };

        $thread{$thread->tid()} = { thread => $thread, trigger => $sem };
    }

    $SIG{TERM} = $SIG{INT} = sub
    {
        $log->say( 'Collect: killed.' );
        map
        {
            $thread{$_}->{thread}->kill( 'SIGKILL' )->detach();
        } keys %thread;

        exit 1;
    };


    $log->say( 'collect: started.' );

    for ( my $now; $now = time; )
    {
        $log->say( 'collect: loop start.' );
        my ( %running, @result );

        map
        {
            die "some threads dead" unless $thread{$_}->{thread}->is_running();
            $thread{$_}->{trigger}->up();
            $running{$_} = 1;
        } keys %thread;

        while( 1 )
        {
            while( my ( $done, $result ) = $finishq->dequeue_nb( 2 ) )
            {
                delete $running{$done};
                push @result, $result if $result;
            }

            last unless %running;

            if ( time - $now > $timeout ) 
            {
                map
                { 
                    $thread{$_}->{thread}->kill( 'SIGALRM' );
                } keys %running;
            }

            sleep 0.5;
        }

        ## sleep sometime until $interval * 3/4 from $now
        my $due = $interval * 3 / 4 + $now - time;
        sleep $due if $due > 0;

        $now = time;
        if( @result )
        {
            ## insert into mysql
            my $ms = MColPro::SqlBase->new( $self->{config} );
            my $data = MColPro::Record->new( $self->{config}, $ms );
            my $exclude = MColPro::Exclude->new( $self->{config}, $ms );
            my $exclude_hash = $exclude->dump( );
            for my $result ( @result )
            {
                map
                {
                    $data->insert( %$_ )
                        unless( $exclude_hash->{cluster}{$_->{cluster}}
                            || $exclude_hash->{$_->{cluster}}{$_->{node}} );
                } @$result;
            }
            $ms->close();
        }
        $log->say( 'collect: loop done.' );

        $due = $interval * 1 / 4 + $now - time;
        sleep $due if $due > 0;
    }
}

1;
