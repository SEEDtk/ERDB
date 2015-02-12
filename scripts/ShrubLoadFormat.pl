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
use Tracer;
use Shrub;
use ShrubLoader;

=head1 Display Shrub Table Load Formats

    ShrubLoadFormat.pl [options]

=head2 Introduction

This script displays in the form of a text file the information needed to
create load files for the specified Shrub database.

=head2 Parameters

There are no positional parameters.

The command-line options are those found in L<Shrub/new_for_script> plus
the following.

=item entities

If specified, name of a file containing a list of entities. Only tables
related to the entities will be displayed.

=back

=cut

    $| = 1; # Prevent buffering on STDOUT.
    # Connect to the database.
    my ($shrub, $opt) = Shrub->new_for_script('%c %o', {},
            ["entities", "If specified, the name of a file containing a list of entities of interest"]);
    # Create the loader helper object.
    my $loader = ShrubLoader->new($shrub);
    # Get the hash of entities.
    my $entityHash = $shrub->GetObjectsTable('entity');
    # Get the list of entities of interest.
    my $entities = {};
    if ($opt->entities) {
        $entities = { map { $_ => $entityHash->{$_} } $loader->GetNamesFromFile($opt->entities, 'entity name') };
    } else {
        $entities = \%$entityHash;
    }
    # Loop through the list of entities.
    for my $entity (sort keys %$entities) {
        # Display the entity description.
        DisplayObject($entity, $entities);
        # Space before the next entity.
        print "\n";
    }
    # Loop through the list of relationships.
    my $relationshipHash = $shrub->GetObjectsTable('relationship');
    for my $relationship (sort keys %$relationshipHash) {
        # Get the FROM and TO entites.
        my $from = $relationshipHash->{$relationship}->{from};
        my $to = $relationshipHash->{$relationship}->{to};
        # Only display this relationship if both ends are in our
        # list of entities.
        if (exists $entities->{$from} && exists $entities->{$to}) {
            DisplayObject($relationship, $relationshipHash);
            # Space before the next relationship.
            print "\n";
        }
    }

# Display the data about an object and its relations.
sub DisplayObject {
    my ($object, $objectHash) = @_;
    FormatNotes($object, $objectHash->{$object}->{Notes}->{content});
    print "\n";
    # Loop through its relations.
    my $relHash = $objectHash->{$object}->{Relations};
    for my $table (sort keys %$relHash) {
        print "    Table: $table\n";
        # Get this table's fields.
        my $relData = $relHash->{$table};
        # Loop through them.
        for my $fieldData (@{$relData->{Fields}}) {
            # Get the field's name.
            my $name = $fieldData->{name};
            # Get the field's type.
            my $type = $fieldData->{type};
            # Display this field's information.
            FormatNotes("        $name ($type)", $fieldData->{Notes}->{content});
        }
        # Space before the next table.
        print "\n";
    }
}

# Display an object with its formatted notes.
sub FormatNotes {
    my ($heading, $notes) = @_;
    # Create the display prefix from the heading.
    my $prefix = "$heading:";
    # Compute the length of the prefix.
    my $length = length $prefix;
    # Create the prefix for secondary lines.
    my $spacer = " " x $length;
    # Delete all the markers from the notes.
    $notes =~ s/\[[^\]]+\]//g;
    # Break the notes into words.
    my @words = split /(?:\s|\n)+/, $notes;
    # Form the words into lines.
    my @line = $prefix;
    my $lineLength = $length;
    for my $word (@words) {
        push @line, $word;
        $lineLength += 1 + length $word;
        if ($lineLength >= 75) {
            print join(" ", @line) . "\n";
            @line = ($spacer);
            $lineLength = $length;
        }
    }
    if (scalar @line > 1) {
        print join(" ", @line) . "\n";
    }
}
