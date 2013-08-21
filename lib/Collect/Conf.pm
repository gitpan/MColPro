package Collect::Conf;

=head1 NAME

Collect::Conf - parse collect configuration

=cut

use strict;
use warnings;

use Carp;
use YAML::XS;
use Util::TimeHelper;

our @PARAM = qw( target plugin type );
our %PARAM = ( thread => 1, interval => 0 );

sub new
{
    my ( $class, $conf ) = splice @_;

    confess "undefined config" unless $conf;
    $conf = readlink $conf if -l $conf;

    my $error = "invalid config $conf";
    confess "$error: not a regular file" unless -f $conf;

    eval { $conf = YAML::XS::LoadFile( $conf ) };

    confess "$error: $@" if $@;
    confess "$error: not HASH" if ref $conf ne 'HASH';

    my $self = bless $conf, ref $class || $class;

    $self->check();

    return $self;
}

sub check
{
    my $self = shift;
    map { die "$_ not defined" if ! $self->{$_} } @PARAM;
    map { $self->{$_} ||= $PARAM{$_} } keys %PARAM;
    $self->{interval} = TimeHelper::rel2sec( $self->{interval} );
    $self->{timeout} = TimeHelper::rel2sec( $self->{interval} / 2 );
}

1;
