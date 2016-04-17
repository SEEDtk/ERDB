#
# Copyright (c) 2003-2015 University of Chicago and Fellowship
# for Interpretations of Genomes. All Rights Reserved.
#
# This file is part of the SEED Toolkit.
#
# The SEED Toolkit is free software. You can redistribute
# it and/or modify it under the terms of the SEED Toolkit
# Public License.
#
# You should have received a copy of the SEED Toolkit Public License
# along with this program; if not write to the University of Chicago
# at info@ci.uchicago.edu or the Fellowship for Interpretation of
# Genomes at veronika@thefig.info or download a copy from
# http://www.theseed.org/LICENSE.TXT.
#


package Shrub::PostLoader;

    use strict;
    use warnings;
    use BasicLocation;
    use ERDBtk::ID::Counter;

=head1 Post-Processing Loader Utilities

This object contains methods for performing the post-processing load tasks. It contains the following fields.

=over 4

=item loader

A L<Shrub::DBLoader> object for manipulating the database and the repository.

=item maxGap

The maximum permissible distance between two features for them to be considered part of a cluster.

=item exclusive

TRUE if we have exclusive database access, else FALSE.

=item uniPercent

Percentage of genomes in which a function must occur singly in order to be considered a universal protein.

=back

=head2 Special Methods

=head3 new

    my $loader = Shrub::PostLoader->new($loader, %options);

Create a new post-processing loader object.

=over 4

=item loader

L<Shrub::DBLoader> object for accessing the database and statistics.

=item options

A hash containing options, including zero or more of the following.

=over 8

=item maxGap

Maximum permissible distance between feature midpoints for two features to be considered part of a cluster. The default
is C<2000>.

=item exclusive

If TRUE, then it is assumed we have exclusive access to the database and significant optimization
is possible. If FALSE, then the operations will be designed to allow concurrent update. The default
is FALSE.

=item uniPercent

Percentage of genomes in which a function must occur singly in order to be considered a universal protein.
The default is C<90>.

=back

=back

=cut

sub new {
    my ($class, $loader, %options) = @_;
    # Create the object.
    my $retVal = {
        loader => $loader,
        maxGap => ($options{maxGap} // 2000),
        exclusive => ($options{exclusive} // 0),
        uniPercent => ($options{uniPercent} // 90),
    };
    # Bless and return it.
    bless $retVal, $class;
    return $retVal;
}


=head2 Public Methods

=head3 LoadClusters

    $loader->LoadClusters($genome);

Load the feature clusters for the specified genome into the database. A feature cluster consists of two
or more features in the same subsystem row that are physically close on a contig. We look for all pairs
of close features in the same subsystem row and form a transitive closure.

=over 4

=item genome

ID of the genome whose clusters are to be loaded.

=back

=cut

sub LoadClusters {
    my ($self, $genome, $maxGap) = @_;
    # Get the database object and the statistics object.
    my $shrub = $self->{loader}->db;
    my $stats = $self->{loader}->stats;
    # Create a counter object for computing cluster IDs.
    my $idHelper = ERDBtk::ID::Counter->new(Cluster => $self->{loader}, $stats,
            exclusive => $self->{exclusive});
    # Create a map of features to subsystem rows.
    my %pegsToRows;
    my $q = $shrub->Get('Feature2Cell Cell2Row', 'Feature2Cell(from-link) LIKE ?',
            ["fig|$genome.%"], 'Feature2Cell(from-link) Cell2Row(to-link)');
    while (my $record = $q->Fetch()) {
        my ($fid, $row) = $record->Values(['Feature2Cell(from-link)', 'Cell2Row(to-link)']);
        $stats->Add(clusterFidRowLink => 1);
        $pegsToRows{$fid}{$row} = 1;
    }
    # Get the features sorted by location.
    my @fidLocs;
    for my $fid (keys %pegsToRows) {
        $stats->Add(clusterFid => 1);
        my $loc = $shrub->loc_of($fid);
        if (! $loc) {
            $stats->Add(clusterMissingLocation => 1);
        } else {
            push @fidLocs, [$fid, $loc];
        }
    }
    # Only continue if we have at least two features.
    if (scalar(@fidLocs) > 1) {
        @fidLocs = sort { BasicLocation::Cmp($a->[1], $b->[1]) } @fidLocs;
        # Now we loop through the features, forming clusters. Each feature can potentially be clustered with the previous one.
        # We continue a cluster until we reach a break. We start with the first feature.
        my ($fid0, $loc0) = @{shift @fidLocs};
        my $midPoint0 = ($loc0->Left + $loc0->Right) / 2;
        # This hash maps each subsystem row to the current cluster ID. If the previous feature was not in a cluster, it maps
        # the row ID to undef.
        my %ssRowClusterId;
        # This hash maps each subsystem row to a list of the features in the current cluster.
        my %ssRowClusterPeg;
        # Process the first feature.
        for my $row (keys %{$pegsToRows{$fid0}}) {
            $ssRowClusterId{$row} = $idHelper->NextID;
            $ssRowClusterPeg{$row} = [$fid0];
        }
        # Loop through the remaining features.
        for my $fidPair (@fidLocs) {
            my ($fid1, $loc1) = @$fidPair;
            $stats->Add(clusterFidProcessed => 1);
            my $midPoint1 = ($loc1->Left + $loc1->Right) / 2;
            # Do we match the previous feature?
            my $distance = abs($midPoint1 - $midPoint0);
            my $close;
            if ($distance <= $self->{maxGap}) {
                $close = 1;
                $stats->Add(clusterFidsClose => 1);
            } else {
                $stats->Add(clusterFidsFar => 1);
            }
            # Get the rows for the new feature.
            my $rowH = $pegsToRows{$fid1};
            # Process all the rows belonging to the old feature.
            for my $row (keys %ssRowClusterId) {
                my $pegList = $ssRowClusterPeg{$row};
                if ($close && $rowH->{$row}) {
                    push @$pegList, $fid1;
                } else {
                    $self->CloseCluster($row, $ssRowClusterId{$row}, $pegList);
                    delete $ssRowClusterId{$row};
                }
            }
            # Process the rows belonging to the new feature. We need to
            # open any that are not already in progress.
            for my $row (keys %$rowH) {
                if (! $ssRowClusterId{$row}) {
                    $ssRowClusterId{$row} = $idHelper->NextID;
                    $ssRowClusterPeg{$row} = [$fid1];
                }
            }
            # Set up for the next pass.
            ($fid0, $midPoint0) = ($fid1, $midPoint1);
        }
        # Close off the remaining clusters.
        for my $row (keys %ssRowClusterId) {
            $self->CloseCluster($row, $ssRowClusterId{$row}, $ssRowClusterPeg{$row});
        }
    }
}

=head3 SetUniRoles

    $loader->SetUniRoles();

Update the universal role flags for all the functions.

=cut

sub SetUniRoles {
    my ($self) = @_;
    # Get the database object and the statistics object.
    my $shrub = $self->{loader}->db;
    my $stats = $self->{loader}->stats;
    # This will count the finds for each function.
    my %funFinds;
    # Loop through the genomes. Notice we count them along the way.
    my @genomes = $shrub->GetAll('Genome', '', [], 'id name');
    my $gCount = 0;
    for my $genome (@genomes) {
        my ($genomeID, $name) = @$genome;
        print "Searching for universal roles in $genomeID: $name.\n";
        # Find all the singletons.
        my %funCount;
        my @funs = $shrub->GetFlat('Feature2Function', 'Feature2Function(from-link) LIKE ? AND Feature2Function(security) = ?',
            ["fig|$genomeID.peg.%", 0], 'Feature2Function(to-link)');
        for my $fun (@funs) {
            $funCount{$fun}++;
            $stats->Add(uniProteinCheck => 1);
        }
        # Count them.
        for my $fun (keys %funCount) {
            if ($funCount{$fun} == 1) {
                $funFinds{$fun}++;
                $stats->Add(uniProteinCandidate => 1);
            } else {
                $stats->Add(uniProteinRejected => 1);
            }
        }
        $gCount++;
    }
    # Compute the threshhold.
    print "$gCount genomes found.\n";
    my $min = $gCount * $self->{uniPercent} / 100;
    print "Universal role threshhold is $min.\n";
    # Now all the genomes have been processed. Update the functions.
    my @funs = $shrub->GetFlat('Function', '', [], 'id');
    for my $fun (@funs) {
        my $count = $funFinds{$fun} // 0;
        my $universal = 0;
        if ($count >= $min) {
            $stats->Add(uniProteinFound => 1);
            $universal = 1;
        }
        $shrub->UpdateEntity(Function => $fun, universal => $universal);
    }
    print "Universal role flags updated.\n";
}

=head2 Internal Methods

=head3 CloseCluster

    $loader->CloseCluster($rowID, $clusterID, \@clusterPegs);

Output a cluster to the database.

=over 4

=item rowID

The ID of the subsystem row to which the cluster belongs.

=item clusterID

The ID to assign to the cluster.

=item clusterPegs

Reference to a list of the features that belong in the cluster.

=back

=cut

sub CloseCluster {
    my ($self, $rowID, $clusterID, $clusterPegs) = @_;
    # Get the loader object and the statistics object.
    my $loader = $self->{loader};
    my $stats = $loader->stats;
    # Only proceed if the cluster has at last two entries.
    if (scalar(@$clusterPegs) >= 2) {
        $stats->Add(clusterOut => 1);
        # Create the cluster itself.
        $loader->InsertObject('Cluster', id => $clusterID, Row2Cluster_link => $rowID);
        # Connect the features.
        for my $peg (@$clusterPegs) {
            $loader->InsertObject('Cluster2Feature', 'from-link' => $clusterID, 'to-link' => $peg);
        }
    }
}

1;