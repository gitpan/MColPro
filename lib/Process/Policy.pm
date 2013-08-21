package Process::Policy;

=head1 NAME

Process::Policy - parse report policy

=cut

use strict;
use warnings;

use YAML::XS;
use POSIX qw( INT_MAX );
use Carp;

use lib 'lib';
use Util::TimeHelper;

sub parse
{
    my $policy = shift;
    my %policy;

    confess "invaild policy conf" unless $policy && ref $policy eq 'ARRAY';

    map
    {
        if( $_->{count} )
        {
            my $count = $_->{count};
            $count =~ s/\s//g;
            my ( $due, $step ) = split ':', $count;
            confess "invaild policy conf" unless $due;
            $step ||= 1;
            my ( $start, $end ) = split '-', $due;
            confess "invaild policy conf" unless $start && $end;
            $_->{count} = [ $start + 0, $end eq '*' ? INT_MAX : $end + 0 ];
            $_->{step} = $step + 0;
        }
        else
        {
            $_->{count} = [ 0, INT_MAX ];
            $_->{step} = 1;
        }

        if( $_->{reciver} )
        {
            my $reciver = $_->{reciver};
            $reciver =~ s/\s//g;
            $_->{reciver} = [ split ',', $reciver ];
        }
        else
        {
            $_->{reciver} = [];
        }
        push @{ $policy{stair} }, $_;
    } @$policy;

    return \%policy;
}

1;
