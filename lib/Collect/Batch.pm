package Collect::Batch;

=head1 NAME

Collect::Batch - Batch targets

=cut

use strict;
use warnings;

use Carp;

sub batch
{
    my ( $target, $thread, @batch ) = @_;
    my $count = 0;

    while( my ( $cluster, $config ) = each %$target )
    {
        ## nodes
        if( $cluster eq 'NODE' )
        {
            map
            {
                push @batch, 
                +{
                    cluster  => $config->{$_}{cluster} || 'none',
                    nodes    => $_,
                    config   => $config->{$_},
                };
                delete $config->{$_}{cluster};
            } keys %$config;

            next;
        }

        ## cluster
        $cluster =~ s/\s//g;
        my @cluster = split ',', $cluster;
        my @config;

        if( ref $config eq 'ARRAY' )
        {
            push @config, @$config;
        }
        elsif ( ref $config eq 'HASH' )
        {
            push @config, $config;
        }
        else
        {
            confess "invaild target, must be HASH or ARRAY";
        }

        map
        {
            for my $c ( @cluster )
            {
                push @batch, 
                +{
                    cluster => $c,
                    config  => $_,
                };
            }
            delete $_->{class};
        } @config;
    }

    my $result = [];
    for ( my $i = 0; @batch; )
    {
        push @{ $result->[ $i ++ % $thread ] }, shift @batch;
    }

    return $result;
}

1;
