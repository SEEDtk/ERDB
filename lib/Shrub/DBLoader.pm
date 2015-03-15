#!/usr/bin/perl -w

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


package Shrub::DBLoader;

    use strict;
    use base qw(RepoLoader);
    use SeedUtils;
    use Digest::MD5;
    use Carp;

=head1 Shrub Load Utilities

=head2 Introduction

This object manages simple utilities for loading the Shrub database. It contains the following
fields.

=over 4

=item shrub

The L<Shrub> object itself.

=item tables

Reference to a hash mapping each object being loaded to a table management object. The table management object
contains the file handles for the object's load files (in the B<handles> member), the maps for the object's
relations (in the B<maps> member), and the relation names (in the B<names> member). Each of these members is
coded as a list reference, in parallel order.

=item tableList

Reference to a list of the tables in the order they are supposed to be loaded, which is the order
they were passed in to L</Open>.

=item hashes

Reference to a hash mapping entity names to hashes that cache the content of the entity. The entity must
be of the type that stores a string that is identified by a UUID. For each entity, this hash
contains the unqualified name of the text field and a sub-hash that maps MD5s of the text field to IDs.
If the string is already in the database, the hash can be used to retrieve the ID; otherwise, we know
we need to add the string to the database.

=item replaces

Reference to a hash containing the names of the tables being inserted in replace mode.

=item closeQueue

Reference to a list of objects that should be closed before this object is closed.

=back

=head2 Special Methods

=head3 new

    my $loader = Shrub::DBLoader->new($shrub);

Create a new, blank loader object.

=over 4

=item shrub

L<Shrub> object for the database being loaded.

=back

=cut

sub new {
    # Get the parameters.
    my ($class, $shrub) = @_;
    # Create the base object.
    my $retVal = Loader::new($class);
    # Attach the Shrub database and denote no tables are being loaded
    # yet.
    $retVal->{shrub} = $shrub;
    $retVal->{hashes} = {};
    $retVal->{tables} = {};
    $retVal->{replaces} = {};
    $retVal->{tableList} = [];
    $retVal->{closeQueue} = [];
    # Return the completed object.
    return $retVal;
}

=head2 Access Methods

=head3 db

    my $shrub = $loader->db;

Return the L<Shrub> database object.

=cut

sub db {
    my ($self) = @_;
    return $self->{shrub};
}


=head2 Database Utility Methods


=head3 CheckByName

    my $id = $loader->CheckByName($entity, $field => $name, $entityHash);

Check to determine if a particular entity instance is in the database. This is similar to L<ERDB/Exists> except
the check is performed based on a unique name field instead of the ID field and the caller can
optionally specify a reference to a hash that maps names to IDs. If this is the case, the hash
will be used instead of the database. The return value is the named instance's ID.

=over 4

=item entity

Name of the entity to check.

=item field

Name of the field containing the names of the entity instances.

=item id

Name of the instance for which to look.

=item entityHash

If specified, a reference to a hash whose keys are all the names of the entity in the database
and which maps those names to IDs. If unspecified, the database will be interrogated directly.

=item RETURN

Returns the ID of the named entity instance, or C<undef> if the entity instance does not
exist.

=back

=cut

sub CheckByName {
    # Get the paramteers.
    my ($self, $entity, $field, $name, $entityHash) = @_;
    # This will be the return value.
    my $retVal;
    # Do we have a hash?
    if ($entityHash) {
        # Yes, check it.
        $retVal = $entityHash->{$name};
    } else {
        # No, check the database.
        ($retVal) = $self->{shrub}->GetFlat($entity, "$entity($field) = ?", [$name], 'id');
    }
    # Return the ID found (if any).
    return $retVal;
}


=head3 CheckCached

    my $found = $loader->CheckCached($entity => $id, $cache);

This method checks to see if the entity instance with the specified ID can be found
in the database. The results of the check will be cached in a client-provided hash
reference to improve performance on future checks.

=over 4

=item entity

The name of the entity type whose instances are to be checked.

=item id

The ID of the entity instance whose existence is in question.

=item cache

Reference to a hash that can be used to improve performance.

=item RETURN

Returns TRUE if the entity instance is in the database, else FALSE.

=back

=cut

sub CheckCached {
    # Get the parameters.
    my ($self, $entity, $id, $cache) = @_;
    # This will be set to TRUE if we confirm the entity instance is in the database.
    my $retVal = $cache->{$id};
    # Was there a result in the cache?
    if (! defined $retVal) {
        # No, check the database.
        $retVal = $self->db->Exists($entity => $id);
        # Save the result in the cache. Note that we can't allow
        # undef, since that's how we detect missing values.
        $cache->{$id} = ($retVal ? 1 : 0);
    }
    # Return the determination indicator.
    return $retVal;
}


=head2 Table-Loading Utility Methods

=head3 Clear

    $loader->Close(@tables);

Clear the database relations for the specified objects.

=over 4

=item tables

List of the names of the objects whose data is to be cleared from the database.

=back

=cut

sub Clear {
    # Get the parameters;
    my ($self, @tables) = @_;
    # Get the database object.
    my $shrub = $self->{shrub};
    # Get the statistics object.
    my $stats = $self->{stats};
    # Loop through the tables specified.
    for my $table (@tables) {
        # Get the descriptor for this object.
        my $object = $shrub->FindEntity($table);
        if ($object) {
            $stats->Add(entityClear => 1);
        } else {
            $object = $shrub->FindRelationship($table);
            if (! $object) {
                die "$table is not a valid entity or relationship name.";
            } else {
                $stats->Add(relationshipClear => 1);
            }
        }
        print "Clearing $table.\n";
        # Get the hash of relations.
        my $relHash = $object->{Relations};
        # Loop through them.
        for my $rel (keys %$relHash) {
            # Recreate this relation.
            $shrub->CreateTable($rel);
            print "$rel recreated.\n";
            $stats->Add(tableClear => 1);
        }
    }
}

=head3 Open

    $loader->Open(@tables);

Open the load files for one or more entities and/or relationships.

=over 4

=item tables

List of the names of the objects to be loaded.

=back

=cut

sub Open {
    # Get the parameters.
    my ($self, @tables) = @_;
    # Get the database object.
    my $shrub = $self->{shrub};
    # Get the statistics object.
    my $stats = $self->{stats};
    # Get the current tables hash and list.
    my $tableH = $self->{tables};
    my $tableL = $self->{tableList};
    # Compute the load directory.
    my $loadDir = $shrub->LoadDirectory();
    # Loop through the tables specified.
    for my $table (@tables) {
        # Only proceed if this table is not already set up.
        if (exists $tableH->{$table}) {
            warn "$table is being opened for loading more than once.\n";
            $stats->Add(duplicateOpen => 1);
        } else {
            # The file handles will be put in here.
            my @handles;
            # The relation maps will be put in here.
            my @maps;
            # The relation names will be put in here.
            my @names;
            # Get the descriptor for this object.
            my $object = $shrub->FindEntity($table);
            if ($object) {
                $stats->Add(entityOpen => 1);
            } else {
                $object = $shrub->FindRelationship($table);
                if (! $object) {
                    die "$table is not a valid entity or relationship name.";
                } else {
                    $stats->Add(relationshipOpen => 1);
                }
            }
            print "Opening $table.\n";
            # Get the hash of relations.
            my $relHash = $object->{Relations};
            # Loop through them.
            for my $rel (keys %$relHash) {
                # Get this relation's field descriptor.
                push @maps, $relHash->{$rel}{Fields};
                # Open a file for it.
                my $fileName = "$loadDir/$rel.dtx";
                open(my $ih, ">$fileName") || die "Could not open load file $fileName: $!";
                $stats->Add(fileOpen => 1);
                push @handles, $ih;
                # Save its name.
                push @names, $rel;
                print "$rel prepared for loading.\n";
            }
            # Store the load information.
            $tableH->{$table} = { handles => \@handles, maps => \@maps, names => \@names };
            push @$tableL, $table;
        }
    }
}

=head3 ReplaceMode

    $loader->ReplaceMode(@tables);

Denote that the specified objects should be processed in replace mode instead of ignore mode. In
replace mode, inserted rows replace existing duplicate rows rather than being discarded.

=over 4

=item tables

List of the names of the entities and relationships to be processed in replace mode.

=back

=cut

sub ReplaceMode {
    # Get the parameters.
    my ($self, @tables) = @_;
    # Get the replace-mode hash.
    my $repHash = $self->{replaces};
    # Loop through the object names.
    for my $table (@tables) {
        # Mark this object as replace mode.
        $repHash->{$table} = 1;
    }
}

=head3 InsertObject

    $loader->InsertObject($table, %fields);

Insert the specified object into the load files.

=over 4

=item table

Name of the object (entity or relationship) being inserted.

=item fields

Hash mapping field names to values. Multi-value fields are passed as list references. All fields should already
be encoded for insertion.


=back

=cut

sub InsertObject {
    # Get the parameters.
    my ($self, $table, %fields) = @_;
    # Get the statistics object.
    my $stats = $self->{stats};
    # Get the load object for this table.
    my $loadThing = $self->{tables}{$table};
    # Are we loading this object using a load file?
    if (! $loadThing) {
        # No, we must insert it directly. Get the database object.
        my $shrub = $self->{shrub};
        # Compute the duplicate-record mode.
        my $dup = ($self->{replaces}{$table} ? 'replace' : 'ignore');
        $shrub->InsertObject($table, \%fields, encoded => 1, dup => $dup);
        $stats->Add("$table-insert" => 1);
    } else {
        # Yes, we need to output to the load files. Loop through the relation tables in the load thing.
        my $handles = $loadThing->{handles};
        my $maps = $loadThing->{maps};
        my $names = $loadThing->{names};
        my $n = scalar @$handles;
        for (my $i = 0; $i < $n; $i++) {
            my $handle = $handles->[$i];
            my $map = $maps->[$i];
            # Figure out if this is the primary relation.
            if ($names->[$i] eq $table) {
                # It is. Loop through the fields of this relation and store the values in here.
                my @values;
                for my $field (@$map) {
                    # Check for the field in the field hash.
                    my $name = $field->{name};
                    my $value = $fields{$name};
                    if (! defined $value && ! $field->{null}) {
                        # We have a missing field value, and we need to identify it. Start with the table name. Add
                        # an ID if we have one.
                        my $tName = $table;
                        if (defined $fields{id}) {
                            $tName = "$tName record $fields{id}";
                        } elsif (defined $fields{'from-link'}) {
                            $tName = "$tName for " . $fields{'from-link'} . " to " . $fields{'to-link'};
                        }
                        confess "Missing value for $name in $tName.";
                    } else {
                        # Store this value.
                        push @values, $value;
                    }
                }
                # Write the primary record.
                print $handle join("\t", @values) . "\n";
                $stats->Add("$table-record" => 1);
            } else {
                # Here we have a secondary relation. A secondary always has two fields, the ID and a multi-value
                # field which will come to us as a list.
                my $id = $fields{id};
                if (! defined $id) {
                    die "ID missing in output attempt of $table.";
                }
                # Get the secondary value.
                my $values = $fields{$map->[1]{name}};
                # Insure it is a list.
                if (! defined $values) {
                    $values = [];
                } elsif (ref $values ne 'ARRAY') {
                    $values = [$values];
                }
                # Loop through the values, writing them out.
                for my $value (@$values) {
                    print $handle "$id\t$value\n";
                    $stats->Add("$table-value" => 1);
                }
            }
        }
    }
}

=head3 QueueSubObject

    $loader->QueueSubObject($subObj);

Add an object to the queue of objects to be closed during cleanup. This allows other objects to
do any preliminary cleanup. Such objects must allow for the possibility of being closed
multiple times. After the first time, the close should have no effect.

=over 4

=item subObj

Object to add to the queue.

=back

=cut

sub QueueSubObject {
    # Get the parameters.
    my ($self, $subObj) = @_;
    my $queue = $self->{closeQueue};
    push @$queue, $subObj;
}

=head3 Close

    $loader->Close();

Close and load all the load files being created.

=cut

sub Close {
    # Get the parameters.
    my ($self) = @_;
    # Get the database object.
    my $shrub = $self->{shrub};
    # Get the load directory.
    my $loadDir = $shrub->LoadDirectory();
    # Get the replace-mode hash.
    my $repHash = $self->{replaces};
    # Get the statistics object.
    my $stats = $self->{stats};
    # Get the load hash and the list of tables.
    my $loadThings = $self->{tables};
    my $loadList = $self->{tableList};
    # Close any sub-objects that need close processing.
    for my $subObject (@{$self->{closeQueue}}) {
        $subObject->Close();
    }
    # Loop through the objects being loaded.
    for my $table (@$loadList) {
        my $loadThing = $loadThings->{$table};
        # Loop through the relations for this object.
        my $names = $loadThing->{names};
        my $handles = $loadThing->{handles};
        my $dups = $loadThing->{dups};
        my $n = scalar @$names;
        for (my $i = 0; $i < $n; $i++) {
            my $name = $names->[$i];
            my $handle = $handles->[$i];
            # Close the file.
            close $handle;
            # Compute the duplicate-record mode.
            my $dup = ($repHash->{$name} ? 'replace' : 'ignore');
            # Load it into the database.
            print "Loading $name.\n";
            my $newStats = $shrub->LoadTable("$loadDir/$name.dtx", $name, dup => $dup);
            # Merge the statistics.
            $stats->Accumulate($newStats);
        }
    }
}

1;
