#!/usr/bin/env perl

#
# Copyright (c) 2003-2006 University of Chicago and Fellowship
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

    use strict;
    use Stats;
    use Tracer;
    use Shrub;


=head1 Verify Relationship Integrity in a Shrub Database

    ShrubFixRels [options] dbName rel1 rel2 ...

This script analyzes relationships in a Shrub database and deletes instances that
link to entities that do not exist.

=head2 Paraameters

The positional parameters are the relationship names.

The command-line options are those described in L<Shrub/new_for_script> plus the
following.

=over 4

=item all

Process all relationships. If this option is specified, no relationship names should be
specified on the command line.

=back

=cut

    $| = 1; # Prevent buffering on STDOUT.
    # Connect to the database.
    my ($shrub, $opt) = Shrub->new_for_script('%c %o', {}, ["all|a", "process all relationships"]);
    # Create the statistics object.
    my $stats = Stats->new();
    # Get the relationship names.
    my @rels;
    if ($opt->all) {
        # Here the user wants all relationships in the database. Insure no relationships have
        # been specified on the command line.
        if (scalar @ARGV) {
            die "ALL specified along with listed relationship names. Use one or the other.";
        }
        # Get the list of relationship names.
        @rels = $shrub->GetRelationshipTypes();
    } elsif (! scalar @rels) {
        # Here there is nothing to do.
        die "No relationships specified for processing.";
    } else {
        # Here the user specified a list of relationship names.
        @rels = @ARGV;
    }
    # Loop through the list of relationships.
    for my $rel (@rels) {
        print "Processing $rel.\n";
        my $subStats = $shrub->FixRelationship($rel);
        $stats->Accumulate($subStats);
    }
    # Denote we're done.
    print "All done.\n" . $stats->Show();
