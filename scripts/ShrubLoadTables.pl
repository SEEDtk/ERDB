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

=head1 Load Shrub Tables from Pre-existing Load Files

    ShrubLoadTables.pl [options] table1 table2 ...

This method will load tables in the Shrub database from pre-existing load files.
It can be used to recover from errors that prevent one of the various load
scripts (e.g. L<ShrubLoadGenomes.pl>, L<ShrubLoadSubsystems.pl>) from completing.
The loads will be performed in C<IGNORE> mode so that existing records are not
replaced. This may not be the optimal result, but it gets us a good result in
an emergency situation.

=head2 Parameters

The positional parameters are the names of the objects (entities and relationships)
to be loaded.

The command-line options are those specified in L<Shrub/new_for_script> plus the
following:

=over 4

=item objects

If specified, the name of a tab-delimited file containing the names of the objects
to load in the first column. These objects will be loaded in addition to any
specified in the positional parameters.

=back

=cut

    use strict;
    use warnings;
    use Shrub;
    use ShrubLoader;

    $| = 1; # Prevent buffering on STDOUT.
    # Connect to the database.
    print "Connecting to database.\n";
    my ($shrub, $opt) = Shrub->new_for_script('%c %o table1 table2 ...', {},
            ["objects=s", "file containing table list in first column"],
            );
    # Create a loader helper and get the statistics object.
    my $loader = ShrubLoader->new($shrub);
    my $stats = $loader->stats;
    print "Parsing parameters.\n";
    # Get the list of objects on the command line.
    my @objects = @ARGV;
    # Check for a list file.
    if ($opt->objects) {
        # We have one. Read in the names from it.
        push @objects, $loader->GetNamesFromFile($opt->objects, 'table');
    }
    # Insure we have something to do.
    if (! scalar @objects) {
        die "Nothing to load: no parameters and no --objects option.";
    }
    # Now loop through the objects forming a list of table names.
    print "Analyzing tables.\n";
    my @rels;
    for my $object (@objects) {
        $stats->Add(open_objects => 1);
        # Check for an entity of this type.
        my $desc = $shrub->FindEntity($object);
        if ($desc) {
            $stats->Add(open_entities => 1);
        } else {
            # Not an entity, check for a relationship.
            $desc = $shrub->FindRelationship($object);
            if ($desc) {
                $stats->Add(open_relationships => 1);
            } else {
                # Here we can't find the object.
                die "$object not found in the database.";
            }
        }
        # Now $desc is the descriptor for this entity or relationship.
        # Get its list of relations.
        my @tabs = sort keys %{$desc->{Relations}};
        $stats->Add(open_relations => scalar @tabs);
        push @rels, @tabs;
    }
    # Now @rels contains a list of all the relations to load.
    # Get the load directory where we'll find the relation files.
    my $loadDir = $shrub->LoadDirectory();
    print "Reading load files from $loadDir.\n";
    # Loop through the relations.
    for my $rel (@rels) {
        # Compute the load file name.
        my $fileName = "$loadDir/$rel.dtx";
        if (! -f $fileName) {
            print "No load file found for $rel.\n";
            $stats->Add(file_missing => 1);
        } else {
            print "Loading $rel.\n";
            $shrub->LoadTable($fileName, $rel, dup => 'ignore');
            $stats->Add(file_loaded => 1);
        }
    }
    # All done.
    print "All done.\n" . $stats->Show();

