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
use ScriptUtils;
use Stats;
use SeedUtils;
use Shrub;

=head1 Create or Repair a Shrub Database

    ShrubCreate [options]

This script creates the tables in a new Shrub database.  Any existing data in the tables
will be destroyed. (Tables not normally found in a Shrub database, however, will be unaffected.)

This method always uses an external DBD, ignoring the DBD stored in the database (if any).

For security reasons, the command-line parameter C<clear> must be specified to make this script
perform its function. If no parameter is specified, the script does nothing.

=head2 Parameters

There are no positional parameters.

The command-line options are as specified in L<Shrub/script_options> plus
the following.

=over 4

=item clear

If specified, all tables in the database will be recreated.

=item missing

If specified, only tables missing from the database will be created.

=item fixup

If specified, tables in the database not found in the DBD will be deleted, and
tables missing from the database will be created. Tables that have changed and
are empty will be dropped and re-created. Tables that have data in them will
be displayed without being updated. If this option is specified, C<missing>
will be ignored.

=item store

Store the DBD in the database to improve performance.

=back

=cut

$| = 1; # Prevent buffering on STDOUT.
# Parse the command line.
my $opt = ScriptUtils::Opts('', Shrub::script_options(),
        ["clear", "erase all tables"],
        ["missing|m", "only add missing tables"],
        ["fixup|f", "attempt to fix tables to match the DBD (implies \"missing\")", { implies => 'missing' }],
        ["store|s", "store the DBD in the database to improve performance"]);
# Check for mutually exclusive parameters.
if ($opt->missing && $opt->clear) {
    die "Cannot specify --clear with --fixup or --missing.";
}
if (! $opt->missing && ! $opt->clear && ! $opt->store) {
    die "No options specified-- nothing to do.";
}
# Connect to the database, forcing use of an external DBD.
my $shrub = Shrub->new_for_script($opt, externalDBD => 1);
# Create the statistics object.
my $stats = Stats->new();
print "Database definition taken from " . $shrub->GetMetaFileName() . "\n";
# Are we storing the DBD?
if ($opt->store) {
    $shrub->InternalizeDBD();
    print "Database definition stored in database.\n";
}
# Get the database handle.
my $dbh = $shrub->{_dbh};
# Get the relation names.
my @relNames = sort $shrub->GetTableNames();
# The list of changed tables will be kept in here.
my %changed;
# Compute the missing-only flag.
my $missing = $opt->missing;
# Get a list of a tables in the actual database.
my @tablesFound = $dbh->get_tables();
print scalar(@tablesFound) . " tables found in database.\n";
# Is this a fixup?
if ($opt->fixup) {
    # Yes. Denote we only want to create missing tables.
    print "Performing fixup.\n";
    # Create a hash for checking the tables against the schema. The check
    # needs to be case-insensitive.
    my %relHash = map { lc($_) => 1 } @relNames;
    # Loop through the tables in the database, looking for ones to drop.
    for my $table (@tablesFound) {
        $stats->Add(tableChecked => 1);
        if (substr($table, 0, 1) eq "_") {
            # Here we have a system table.
            $stats->Add(systemTable => 1);
        } elsif (! $relHash{lc $table}) {
            # Here the table is not in the DBD.
            print "Dropping $table.\n";
            $dbh->drop_table(tbl => $table);
            $stats->Add(tableDropped => 1);
        } else {
            # Here we need to compare the table's real schema to the DBD.
            print "Analyzing $table.\n";
            # This is the real scheme.
            my @cols = $dbh->table_columns($table);
            # We'll set this to TRUE if there is a difference.
            my $different;
            # Loop through the DBD schema, comparing.
            my $relation = $shrub->FindRelation($table);
            my $fields = $relation->{Fields};
            my $count = scalar(@cols);
            if (scalar(@$fields) != $count) {
                print "$table has a different column count.\n";
                $different = 1;
            } else {
                # The column count is the same, so we do a 1-for-1 compare.
                for (my $i = 0; $i < $count && ! $different; $i++) {
                    # Get the fields at this position.
                    my $actual = $cols[$i];
                    my $schema = $fields->[$i];
                    # Compare the names and the nullabilitiy.
                    if (lc $actual->[0] ne lc ERDB::_FixName($schema->{name})) {
                        print "Field mismatch at position $i in $table.\n";
                        $different = 1;
                    } elsif ($actual->[2] ? (! $schema->{null}) : $schema->{null}) {
                        print "Nullability mismatch in $actual->[0] of $table.\n";
                        $different = 1;
                    } else {
                        # Here we have to compare the field types. Because of
                        # a glitch, we only look at the first word.
                        my ($schemaType) = split m/\s+/, $shrub->_TypeString($schema);
                        if (lc $schemaType ne lc $actual->[1]) {
                            print "Type mismatch in $actual->[0] of $table.\n";
                            $different = 1;
                        }
                    }
                }
            }
            if ($different) {
                # Here we have a table mismatch.
                $stats->Add(tableMismatch => 1);
                # Check for data in the table.
                if ($shrub->IsUsed($table)) {
                    # There's data, so save it for being listed
                    # later.
                    $changed{$table} = 1;
                } else {
                    # No data, so drop it.
                    print "Dropping $table.\n";
                    $dbh->drop_table(tbl => $table);
                    $stats->Add(tableDropped => 1);
                }
            }
        }
    }
}
# If clear is specified, drop all the current tables. Noteb that system tables (which
# begin with an underscore) are not dropped.
if ($opt->clear) {
    for my $relName (@tablesFound) {
        if (substr($relName, 0, 1) ne '_') {
            print "Dropping $relName\n";
            $shrub->DropRelation($relName);
        }
    }
}
# Here is where we create tables. We only do this if clear or missing is
# set.
if ($opt->clear || $opt->missing) {
    # If this is a clear, we don't need to drop the tables before creating.
    my $nodrop = ($opt->clear ? 1  : 0);
    # Loop through the relations.
    print "Processing relations.\n";
    for my $relationName (@relNames) {
        $stats->Add(relationChecked => 1);
        # Do we want to create this table?
        if (! $missing || ! $dbh->table_exists($relationName)) {
            $shrub->CreateTable($relationName, nodrop => $nodrop);
            print "$relationName created.\n";
            $stats->Add(relationCreated => 1);
        } elsif ($changed{$relationName}) {
            print "$relationName needs to be recreated.\n";
            print "Field string: " . $shrub->ComputeFieldString($relationName) . "\n";
        }
    }
}
# Tell the user we're done.
print "Database processed.\n" . $stats->Show();
