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


package ERDB::Utils;

    use strict;
    use warnings;
    use ERDB;
    use Stats;

=head1 ERDB Maintenance Utility Helper

This object provides utility services for the ERDB object that are likely to be of use in
creating maintenance scripts. Unlike normal ERDB methods, the methods of this object
write status messages to the standard output and keep statistics.

This object contains the following fields.

=over 4

=item stats

L<Stats> object for keeping statistics about our activities.

=item erdb

The L<ERDB> object for the relevant database.

=item tables

Reference to a hash of the names of the database tables present at the time of
the first L<GetTableNames> call. The table names are all folded to lower case
in order to compensate for the case-insensitivity of SQL.

=item relNFolder

Reference to a hash that maps an all-lower-case version of each DBD relation name
to its real value. This is necessary to deal with the case-insensitive nature of
SQL table names.

=back

=head2 Special Methods

=head3 new

    my $utils = ERDB::Utils->new($erdb);

Create a new, blank ERDB Maintenance Utility Helper.

=over 4

=item erdb

An L<ERDB> object for accessing the relevant database.

=back

=cut

sub new {
    # Get the parameters.
    my ($class, $erdb) = @_;
    # Create the statistics object.
    my $stats = Stats->new();
    # Get the relation names from the DBD.
    my @rels = $erdb->GetTableNames;
    # Use them to create a relation map.
    my %relNFolder = map { lc $_ => $_ } @rels;
    # Create the object.
    my $retVal = {
        erdb => $erdb,
        stats => $stats,
        relNFolder => \%relNFolder
    };
    # Bless and return it.
    bless $retVal, $class;
    return $retVal;
}

=head3 init_options

    my @opt_specs = ERDB::Utils::init_options()

These are the command-line options for database initialization.

=over 4

=item store

Store the DBD in the database to improve performance.

=item clear

Erase all the tables in the database and recreate them.

=back

=cut

sub init_options {
    return (
    ['store', "store the DBD in the database"],
    ['clear', "clear the database"],
    );
}


=head2 Query Methods

=head3 stats

    my $stats = $utils->stats;

Return the statistics object.

=cut

sub stats {
    return $_[0]->{stats};
}

=head3 GetTableNames

    my $nameHash = $utils->GetTableNames(%options);

Return a hash keyed on the table names (other than system tables) present in the
database. These are the names of tables physically present, not the ones defined
by the DBD.

=over 4

=item options

Hash of options, including zero or more of the following.

=over 8

=item refresh

If TRUE, then the database will be interrogated to get the most current list of
table names. If FALSE, the database will only be interrogated if the table names
are not yet known.

=back

=item RETURN

Returns a hash keyed by table names. The table names have all been folded to
lower case.

=back

=cut

sub GetTableNames {
    my ($self, %options) = @_;
    # Get the database object.
    my $erdb = $self->{erdb};
    # Do we need to ask for the table names?
    if (! $self->{tables} || $options{refresh}) {
        # Yes. Get the DBKernel object.
        my $dbh = $erdb->{_dbh};
        # Ask for the table names. Note we use grep to eliminate system tables and we
        # convert the table name to lower case, because SQL is case-insensitive.
        my %tables = map { lc($_) => 1 } grep { substr($_,0,1) ne '_' } $dbh->get_tables();
        # Store it in this object.
        $self->{tables} = \%tables;
    }
    # Return the stored table name hash.
    return $self->{tables};
}


=head2 Public Manipulation Methods

=head3 Init

    my $cleared = $utils->InitCheck($opt);

This is an ad hoc method that processes the command-line options in L</init_options>. It will
return TRUE if the database was cleared, else FALSE.

=over 4

=item opt

L<Getopt::Long::Descriptive::Opts> object from a call to L<ScriptUtils/Opts> that included
the L</init_options> command-line options. The C<store> and C<clear> options will be processed.

=item RETURN

Returns TRUE if the database was cleared, else FALSE.

=back

=cut

sub Init {
    # Get the parameters.
    my ($self, $opt) = @_;
    # This will be set to TRUE if we clear.
    my $retVal;
    # Get the ERDB object.
    my $erdb = $self->{erdb};
    # Are we storing?
    if ($opt->store) {
        $erdb->InternalizeDBD();
        print "Database definition stored in database.\n";
    }
    # Check for a clear request.
    if ($opt->clear) {
        print "CLEAR option specified.\n";
        # Delete all the current tables.
        $self->DropAll();
        # Recreate the new tables.
        $self->CreateMissing();
        # Denote we cleared.
        $retVal = 1;
    }
    # Return the clear flag.
    return $retVal;
}


=head3 FixDatabase

    my @badTables = $utils->FixDatabase(%options);

Loop through all the tables in the database, attempting as much as possible to correct any
discrepancies with the DBD. This method will not create missing tables (use L</CreateMissing>
for that). It will, however, drop extra tables, correct indexes, and recreate empty tables
with new field definitions.

=over 4

=item options

Hash of options for this operation.

=over 8

=item refresh

If TRUE, then the list of table names stored in this object will be refreshed. If FALSE, it
will be assumed the database still has the same tables it had at the time of the last
call to L</GetTableNames>.

=back

=item RETURN

Returns a list of the mismatched tables that could not be fixed.

=back

=cut

sub FixDatabase {
    # Get the parameters.
    my ($self, %options) = @_;
    # This will contain the list of tables we couldn't fix.
    my @retVal;
    # Get the hash of tables in this database. System tables will not be included.
    my $tableH = $self->GetTableNames(%options);
    # Loop through the tables.
    for my $table (keys %$tableH) {
        # Fix the table.
        my $ok = $self->FixupTable($table);
        # If we couldn't fix the table, put it in the return list.
        if (! $ok) {
            push @retVal, $table;
        }
    }
    # Return the list of unfixed tables.
    return @retVal;
}

=head3 DropAll

    $utils->DropAll(%options);

Drop all the existing (non-system) tables in the database.

=over 4

=item options

Hash of options for this operation.

=over 8

=item refresh

If TRUE, then the list of table names stored in this object will be refreshed. If FALSE, it
will be assumed the database still has the same tables it had at the time of the last
call to L</GetTableNames>.

=back

=back

=cut

sub DropAll {
    # Get the parameters.
    my ($self, %options) = @_;
    # Get the database object.
    my $erdb = $self->{erdb};
    # Get the DBKernel handle.
    my $dbh = $erdb->{_dbh};
    # Get the statistics object.
    my $stats = $self->stats;
    # Get the hash of table names currently in the database.
    my $tableH = $self->GetTableNames(refresh => $options{refresh});
    # Loop through the tables.
    for my $table (keys %$tableH) {
         print "Dropping $table.\n";
         $stats->Add(tableDropped => 1);
         $dbh->drop_table(tbl => $table);
    }
    # Denote all the tables are gone.
    $self->{tables} = {};
}

=head3 CreateMissing

    my @created = $utils->CreateMissing(%options);

Create all the relations defined in the DBD that are not currently found in the
database and return the list of tables created.

=over 4

=item options

Hash of options for this operation.

=over 8

=item refresh

If TRUE, then the list of table names stored in this object will be refreshed. If FALSE, it
will be assumed the database still has the same tables it had at the time of the last
call to L</GetTableNames>.

=back

=back

=cut

sub CreateMissing {
    # Get the parameters.
    my ($self, %options) = @_;
    # We'll return the list of tables created in here.
    my @retVal;
    # Get the database object.
    my $erdb = $self->{erdb};
    # Get the statistics object.
    my $stats = $self->stats;
    # Get the hash of table names currently in the database.
    my $tableH = $self->GetTableNames(refresh => $options{refresh});
    # Get the list of tables in the DBD.
    my @relNames = $erdb->GetTableNames();
    # Loop through the DBD tables.
    for my $relName (@relNames) {
        # Does this table exist in the database?
        if (! $tableH->{lc $relName}) {
            # No, create it.
            print "Creating $relName\n";
            $erdb->CreateTable($relName, nodrop => 1);
            # Insure we know it's present now.
            $tableH->{lc $relName} = 1;
            # Tell the caller we created it.
            push @retVal, $relName;
        }
    }
    # Return the list of tables created.
    return @retVal;
}


=head3 FixupTable

    my $okFlag = FixupExisting($relName);

Fix an existing table as much as possible to match the DBD.

=over 4

=item relName

The name of a table that exists in the database.

=item RETURN

Returns TRUE if the table has been fixed or was already correct, FALSE if the
table needs to be manually adjusted.

=back

=cut

sub FixupTable {
    # Get the parameters.
    my ($self, $relName) = @_;
    # Get the ERDB object.
    my $erdb = $self->{erdb};
    # Get the statistics object.
    my $stats = $self->stats;
    # Get the DBKernel handle.
    my $dbh = $erdb->{_dbh};
    # This will be set to FALSE if we can't fix the table.
    my $retVal = 1;
    # See if the table is in the database.
    my $realName = $self->{relNFolder}{$relName};
    if (! $realName) {
        # It is not. Drop the table.
        print "Dropping table $relName.\n";
        $dbh->drop_table(tbl => $relName);
        $stats->Add(tableDropped => 1);
        # Delete it from the internal table hash, if any.
        my $tableH = $self->{tables};
        if ($tableH) {
            delete $tableH->{$relName};
        }
    } else {
        # Here we need to compare the table's real schema to the DBD.
        print "Analyzing table $realName.\n";
        # Get the table's descriptor.
        my $relation = $erdb->FindRelation($realName);
        # This is the real scheme.
        my @cols = $dbh->table_columns($realName);
        # Loop through the DBD schema, comparing.
        my $fields = $relation->{Fields};
        my $count = scalar(@cols);
        if (scalar(@$fields) != $count) {
            print "$relName has a different column count.\n";
            $retVal = 0;
        } else {
            # The column count is the same, so we do a 1-for-1 compare.
            for (my $i = 0; $i < $count; $i++) {
                # Get the fields at this position.
                my $actual = $cols[$i];
                my $schema = $fields->[$i];
                # Compare the names and the nullabilitiy.
                if (lc $actual->[0] ne lc ERDB::_FixName($schema->{name})) {
                    print "Field mismatch at position $i in $relName.\n";
                    $retVal = 0;
                } elsif ($actual->[2] ? (! $schema->{null}) : $schema->{null}) {
                    print "Nullability mismatch in $actual->[0] of $relName.\n";
                    $retVal = 0;
                } else {
                    # Here we have to compare the field types. Because of
                    # a glitch, we only look at the first word.
                    my ($schemaType) = split m/\s+/, $erdb->_TypeString($schema);
                    if (lc $schemaType ne lc $actual->[1]) {
                        print "Type mismatch in $actual->[0] of $relName.\n";
                        $retVal = 0;
                    }
                }
            }
        }
        # If there is a field mismatch, our only remedy is to recreate the
        # relation. That, in turn, is only possible if the relation is empty.
        if (! $retVal && ! $erdb->IsUsed($realName)) {
            print "Recreating $realName.\n";
            $erdb->CreateTable($realName);
            $stats->Add(tableRecreated => 1);
            # Denote this table is now ok.
            $retVal = 1;
        } else {
            # Check for problems with the indexes. These are all fixable. If
            # the relation is bad, it will still get us closer.
            my $indexH = $relation->{Indexes};
            my $realIndexH = $dbh->show_indexes($realName);
            # This hash will list the desired indexes found to be real. We will use it
            # to decide which need to be created.
            my %idxFound;
            # Loop through the real indexes.
            for my $index (keys %$realIndexH) {
                # Is this an index we want?
                if (! exists $indexH->{$index}) {
                    # No. Drop it.
                    print "Removing index $index.\n";
                    $stats->Add(indexDropped => 1);
                    $dbh->drop_index(tbl => $realName, idx => $index);
                } else {
                    # Here we want the index. We need to verify that it is
                    # correct. If it isn't, we recreate it.
                    my $indexErrors = 0;
                    # If the index is not the primary, check the uniqueness flags.
                    if ($index ne 'PRIMARY') {
                        my $unique = ($indexH->{$index}{unique} || 0);
                        my $realUnique = ($realIndexH->{$index}{unique} || 0);
                        if ($unique != $realUnique) {
                            $indexErrors++;
                            print "Incorrect uniqueness setting for $index.\n";
                            $stats->Add(uniqueMismatch => 1);
                        }
                    }
                    # Check the field lists.
                    my $fieldsL = $indexH->{$index}{IndexFields};
                    my $realFieldsL = $realIndexH->{$index}{fields};
                    my $ndx = scalar @$fieldsL;
                    my $realNdx = scalar @$realFieldsL;
                    if ($ndx > $realNdx) {
                        $indexErrors++;
                        print "$index has too few fields in the database.\n";
                    } elsif ($ndx < $realNdx) {
                        $indexErrors++;
                        print "$index has too many fields in the database.";
                    } else {
                        # Here we have to compare the field names. To make it fair, we need to strip
                        # the modifiers from the DBD definition.
                        my @fields = map { $_ =~ /^(\S+)/; $1 } @$fieldsL;
                        my $i = 0;
                        while ($i < $ndx && $fields[$i] eq $realFieldsL->[$i]) {
                            $i++;
                        }
                        if ($i < $ndx) {
                            print "Field mismatch in $index at position $i.\n";
                            $indexErrors++;
                        }
                    }
                    # If the index does not match, recreate it.
                    if ($indexErrors) {
                        print "Recreating index $index.\n";
                        $stats->Add(recreateIndex => 1);
                        $dbh->drop_index(tbl => $realName, idx => $index);
                        $erdb->CreateIndex($realName, $index);
                    }
                    # Denote that this index is present.
                    $idxFound{$index} = 1;
                }
            }
            # Now loop through the desired indexes, creating the ones that
            # weren't found.
            for my $index (keys %$indexH) {
                if (! $idxFound{$index}) {
                    # We need to create this index.
                    print "Creating index $index.\n";
                    $stats->Add(indexCreated => 1);
                    $erdb->CreateIndex($realName, $index);
                }
            }
        }
        # Record the fact if we couldn't fix the relation.
        if (! $retVal) {
            $stats->Add(tableMismatch => 1);
            $stats->AddMessage("Relation $realName needs to be manually repaired.")
        }
    }
    # Return the determination indicator.
    return $retVal;
}


=head3 FixRelationship

    $utils->FixRelationship($name, %options);

This method scans a relationship and insures that all of the
instances connect to valid entities on both sides. If any instance
fails to connect, it will be deleted. This method determines the
relationship type and calls L</FixRealRelationship> or
L</FixEmbeddedRelationship> as appropriate.

=over 4

=item name

Name of the relationship to scan.

=item options

Hash of options modifying this process. The following keys are possible.

=over 8

=item batchSize

The number of records to process in a batch. The default is C<50>.

=item testOnly

If TRUE, statistics will be accumulated but no records will be deleted.
The default is FALSE.

=back

=back

=cut

sub FixRelationship {
    # Get the parameters.
    my ($self, $name, %options) = @_;
    # Get the relationship descriptor.
    my $relData = $self->{erdb}->FindRelationship($name);
    # Process according to the relationship type.
    if (! $relData) {
        die "Relationship $name not found.";
    } elsif ($relData->{embedded}) {
        $self->FixEmbeddedRelationship($name, %options);
    } else {
        $self->FixRealRelationship($name, %options);
    }
}


=head3 FixEmbeddedRelationship

    $utils->FixEmbeddedRelationship($name, %options);

This method scans an entity with an embedded relationship and
insures that the embedded relationship links point to entity
instances that exist. If they do not, the entity is deleted.
The process can be very database-intensive.

=over 4

=item name

Name of the embedded relationship to scan.

=item options

Hash of options modifying this process. The following keys are possible.

=over 8

=item testOnly

If TRUE, statistics will be accumulated but no records will be deleted.
The default is FALSE.

=back

=back

=cut

sub FixEmbeddedRelationship {
    # Get the parameters.
    my ($self, $name, %options) = @_;
    # Determine whether or not this is test mode.
    my $testOnly = $options{testOnly};
    # Get the database.
    my $erdb = $self->{erdb};
    # Get the statistics object.
    my $stats = $self->stats;
    # Compute the names of the entities on either side.
    my ($fromEntity, $toEntity) = $erdb->GetRelationshipEntities($name);
    # We are going to loop through the to-entities in order by from-link
    # value. The following variable will contain the ID for the last
    # from-index processed.
    my $lastFrom = "";
    # Loop through the to-entities.
    my $done = 0;
    while (! $done) {
        # Get the first to-entity with a new from-key.
        my ($toInstance) = $erdb->GetFlat("$name", "$name(from-link) > ?", [$lastFrom], 'from-link', 1);
        my ($fromID, $id) = @$toInstance;
        # Does the from-entity exist?
        if ($erdb->Exists($fromEntity => $fromID)) {
            # Yes. Record it.
            $stats->Add("$fromEntity-keyFound" => 1);
        } else {
            # No. Prepare to delete the connected to-entities.
            $stats->Add("$fromEntity-keyNotFound" => 1);
            my @toIDs = $erdb->GetFlat("$name", "$name(from-link) = ?", [$fromID], 'to-link');
            # Loop through the to-instances.
            for my $toID (@toIDs) {
                # Delete this to-instance (assuming we are not in testOnly mode).
                $stats->Add("$toEntity-disconnected" => 1);
                if (! $testOnly) {
                    my $subStats = $erdb->Delete($toEntity => $toID);
                    $stats->Accumulate($subStats);
                }
            }
        }
    }
}


=head3 FixRealRelationship

    $utils->FixRealRelationship($name, %options);

This method scans a relationship and insures that all of the
instances connect to valid entities on both sides. If any instance
fails to connect, it will be deleted. The process is fairly
memory-intensive. Note also that it does not work for embedded
relationships. For those, use L</FixEmbeddedRelationship>.

=over 4

=item name

Name of the relationship to scan.

=item options

Hash of options modifying this process. The following keys are possible.

=over 8

=item batchSize

The number of records to process in a batch. The default is C<50>.

=item testOnly

If TRUE, statistics will be accumulated but no records will be deleted.
The default is FALSE.

=back

=back

=cut

sub FixRealRelationship {
    # Get the parameters.
    my ($self, $name, %options) = @_;
    # Compute the batch size.
    my $batchSize = $options{batchSize} // 50;
    # Determine whether or not this is test mode.
    my $testOnly = $options{testOnly};
    # Get the database.
    my $erdb = $self->{erdb};
    # Get the statistics object.
    my $stats = $self->stats;
    # Compute the names of the entities on either side.
    my ($fromEntity, $toEntity) = $erdb->GetRelationshipEntities($name);
    my %entities = (from => $fromEntity, to => $toEntity);
    # Loop through the relationship, saving the from and to
    # entity ids.
    my %idHash = (from => {}, to => {});
    my $query = $erdb->Get($name, "", [], 'from-link to-link');
    while (my $row = $query->Fetch()) {
        my ($from, $to) = $row->Values('from-link to-link');
        $idHash{from}{$from} = 1;
        $idHash{to}{$to} = 1;
        $stats->Add("${name}In" => 1);
    }
    # Now verify that the entities exist. We process each direction
    # separately.
    for my $dir (qw(from to)) {
        my $entity = $entities{$dir};
        # Loop through the entity IDs in this direction.
        # We process them in batches.
        my @idList = ();
        for my $id (sort keys %{$idHash{$dir}}) {
            $stats->Add("key$name$dir" => 1);
            push @idList, $id;
            if (scalar(@idList) >= $batchSize) {
                $self->_ProcessFixRelationshipBatch($name, $entity, $dir, \@idList, $testOnly);
                @idList = ();
            }
        }
        # Process the residual batch (if any).
        if (@idList) {
            $self->_ProcessFixRelationshipBatch($name, $entity, $dir, \@idList, $testOnly);
        }
    }
}

=head2 Internal Methods

=head3 _ProcessFixRelationshipBatch

    $utils->_ProcessFixRelationshipBatch($name, $entity, $dir, $idList, $testOnly);

Delete relationship rows that are disconnected in the specified direction.
A batch of entity keys is passed in, and any relationship instances connecting
to keys that do not have corresponding entities are deleted.

=over 4

=item name

Name of the relevant relationship.

=item entity

Name of the target entity.

=item dir

C<from> if the entity is in the from-direction, C<to> if it is in the to-direction.

=item idList

Reference to a list of entity IDs.

=item testOnly

If TRUE, statistics will be accumulated but no deletions will be performed.

=back

=cut

sub _ProcessFixRelationshipBatch {
    # Get the parameters.
    my ($self, $name, $entity, $dir, $idList, $testOnly) = @_;
    # Get the database and statistics object.
    my $stats = $self->stats;
    my $erdb = $self->{erdb};
    # Construct a query to look up the entity IDs.
    my $n = scalar(@$idList);
    my $filter = "$entity(id) IN (" . join(", ", ('?') x $n) . ")";
    my %keysFound = map { $_ => 1 } $erdb->GetFlat($entity, $filter,
            $idList, 'id');
    $stats->Add("$entity-keyFound" => scalar keys %keysFound);
    $stats->Add("$entity-keyQuery" => 1);
    # Now we format a delete filter for any key we DIDN'T find.
    $filter = "$name($dir-link) = ?";
    # Loop through all the keys.
    for my $id (@$idList) {
        if (! $keysFound{$id}) {
            # Key was not found, so delete its relationship rows.
            $stats->Add("$entity-keyNotFound" => 1);
            if (! $testOnly) {
                my $count = $erdb->DeleteLike($name, $filter, [$id]);
                $stats->Add("$name-delete$dir" => $count);
            }
        }
    }
}


1;