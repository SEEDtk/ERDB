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


package ERDB::ID;

    use strict;
    use warnings;
    use Tracer;

=head1 ERDB ID Helper

An ID helper is used to generate IDs for entities with generated IDs. Each ID helper has a base class and
two subclasess-- one for situations where the client has exclusive access to the database, and one for
situations where the client is sharing the database.

The ID helper works with a I<loader object>. The loader object has two required methods:

=over 4

=item db

Return the L<ERDB> object that connects to the database.

=item InsertObject($entity, %fields)

Insert a new entity instance into the database.

=back

This object contains the following fields.

=over 4

=item checkHash

Reference to a hash table mapping check field values to IDs. This is used for tables where
we have an alternate unique key for the entity instances.

=item entityName

Name of the target entity.

=item checkField

Name of the alternate unique key field, or C<undef> if there is no such field.

=item erdb

L<ERDB> object for accessing the database.

=item loader

Loader object for inserting into the database.

=item exclusive

TRUE if we have exclusive access to the database, else FALSE.

=item stats

A L<Stats> object for tracking statistics about the current run.

=back

=head2 Special Methods

=head3 new

    my $helper = ERDB::ID->new($entityName, $loader, $stats, %options);

Construct a new ID helper object for the specified entity.

=over 4

=item entityName

Name of the entity type that this helper generates IDs for.

=item loader

A loader object used to insert records and access the database.

=item stats

A L<Stats> object used for tracking statistics.

=item options

A hash of options, including zero or more of the following.

=over 8

=item exclusive

If TRUE, then it will be presumed we have exclusive access to the database and certain
optimizations will be possible. The default is FALSE, meaning the data can change on us.

=item checkField

If specified, the name of an alternate key field that uniquely identifies entity instances.
This field can be used to determine if an entity instance already exists in the database.
The default is that no such field exists.

=back

=back

=cut

sub new {
    # Get the parameters.
    my ($class, $entityName, $loader, $stats, %options) = @_;
    # Get the ERDB object.
    my $erdb = $loader->db;
    # Get the check-field name.
    my $checkField = $options{checkField};
    # Create the object.
    my $retVal = {
        entityName => $entityName,
        loader => $loader,
        erdb => $erdb,
        checkField => $options{checkField},
        exclusive => $options{exclusive},
        stats => $stats
    };
    # Bless and return the object.
    bless $retVal, $class;
    return $retVal;
}

=head2 Sublcass Methods

=head3 CheckExclusive

    my $idFound = $helper->CheckExclusive($checkValue);

Find the ID of the specified entity instance based on its check value. If the instance is not
in the database, return C<undef>. This method should only be called if we have exclusive
control of the database. In addition, it will only work if all inserts are performed using
this object's L</Insert> method.

=over 4

=item checkValue

Value of the alternate key or check field in a proposed new entity instance.

=item RETURN

Returns the ID of a matching instance already in the database, or C<undef> if there is none
at the current time.

=back

=cut

sub CheckExclusive {
    # Get the parameters.
    my ($self, $checkValue) = @_;
    # Get the statistics object and the entity name.
    my $stats = $self->{stats};
    my $entityName = $self->{entityName};
    # Look for the entity instance in the check hash.
    my $retVal = $self->{checkHash}{$checkValue};
    $stats->Add($entityName . ($retVal ? 'CheckFound' : 'CheckNotFound') => 1);
    # Return the result.
    return $retVal;
}

=head3 CheckShared

    my $idFound = $helper->CheckShared($checkValue);

Find the ID of the specified entity instance based on its check value. If the instance is not
in the database, return C<undef>. Unlike L</CheckExclusive>, this method does not guarantee
that the object will not be inserted while we are not looking. It is provided solely as an
optimization if duplicates are likely.

=over 4

=item checkValue

Value of the alternate key or check field in a proposed new entity instance.

=item RETURN

Returns the ID of a matching instance already in the database, or C<undef> if there is none
at the current time.

=back

=cut

sub CheckShared {
    # Get the parameters.
    my ($self, $checkValue) = @_;
    # Get the statistics object and the entity name.
    my $stats = $self->{stats};
    my $entityName = $self->{entityName};
    # Look for the entity instance in the database.
    my $erdb = $self->{erdb};
    my $checkField = $self->{checkField};
    my ($retVal) = $erdb->GetFlat($entityName, "$entityName($checkField) = ?", [$checkValue], 'id');
    $stats->Add($entityName . ($retVal ? 'CheckFound' : 'CheckNotFound') => 1);
    # Return the result.
    return $retVal;
}

=head3 db

    my $erdb = $helper->db;

Return the L<ERDB> object for accessing the database.

=cut

sub db {
    return $_[0]->{erdb};
}

=head3 stats

    my $stats = $helper->stats;

Return the L<Stats> object for tracking statistics.

=cut

sub stats {
    return $_[0]->{stats};
}

=head3 loader

    my $loader = $helper->loader;

Return the loader object for inserting records into the database.

=cut

sub loader {
    return $_[0]->loader;
}


=head2 Public Manipulation Methods

=head3 Insert

    my $id = $helper->Insert(%fields);

Insert a new entity instance and return the ID.

=over 4

=item fields

Hash mapping each field name to its value. If the C<id> field has no value, a new ID will be
computed.

=item RETURN

Returns the ID of the new object.

=back

=cut

sub Insert {
    # Get the parameters.
    my ($self, %fields) = @_;
    # Get the statistics object.
    my $stats = $self->{stats};
    # Try to extract the ID.
    my $retVal = $fields{id};
    # Do we already have an ID?
    if ($retVal) {
        # Yes, get the loader object.
        my $loader = $self->{loader};
        # Do a simple insert.
        $stats->Add(insertWithID => 1);
        $loader->InsertObject($self->{entityName}, %fields);
    } else {
        # No, do an InsertNew.
        $stats->Add(insertIDNeeded => 1);
        $retVal = InsertNew(%fields);
        # If we have a checking hash, insert the new object.
        my $checkHash = $self->{checkHash};
        if ($checkHash) {
            $checkHash->{$fields{$self->{checkField}}} = $retVal;
        }
    }
    # Return the new instance's ID.
    return $retVal;
}


=head2 Virtual Methods

=head3 Check

    my $idFound = $helper->Check($checkValue);

Find the ID of the specified entity instance based on its check value. If the instance is not
in the database, return C<undef>.

=over 4

=item checkValue

Value of the alternate key or check field in a proposed new entity instance.

=item RETURN

Returns the ID of a matching instance already in the database, or C<undef> if there is none
at the current time.

=back

=cut

sub Check {
    Confess("Pure virtual ERDB::ID::Check method called.");
}


=head3 InsertNew

    my $newID = $helper->InsertNew(%fields);

Insert a new entity instance into the database and return its ID.

=over 4

=item fields

Hash containing the field values for the new entity instance.

=item RETURN

Returns the ID of the entity instance after it has been inserted.

=back

=cut

sub InsertNew {
    Confess("Pure virtual ERDB::ID::InsertNew method called.");
}


1;