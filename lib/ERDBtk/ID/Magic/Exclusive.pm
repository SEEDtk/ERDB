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


package ERDBtk::ID::Magic::Exclusive;

    use strict;
    use warnings;
    use base qw(ERDBtk::ID::Magic);


=head1 ERDBtk Magic ID Helper for Exclusive Database Access

This is a subclass for L<ERDBtk::ID> that manages magic name IDs for environments when the
client has exclusive access to the database. This allows a great deal of optimization, since
once we load things into memory we don't have to worry about the information becoming obsolete.

In addition to the fields in the super-classes, this object contains the following fields.

=over 4

=item prefixHash

Reference to a hash that maps each existing magic name prefix to the next available suffix
for it.

=item checkHash

If this entity type has a check field, reference to a hash that maps each entity instance's
check field value to the corresponding ID.

=back

=head2 Special Methods

=head3 new

    my $helper = ERDBtk::ID::Magic::Exclusive->new($entityName, $loader, $stats, %options);

Construct a new ID helper object for the specified entity.

=over 4

=item entityName

Name of the entity type that this helper generates IDs for.

=item loader

A loader object used to insert records and access the database.

=item stats

A L<Stats> object used for tracking statistics.

=item options

A hash of options. In addition to the options used by the super-classes, this can
include

=over 8

=item hashes

Reference to a list that contains the prefix hash and (if applicable) check hash
for this entity type. If omitted, the hashes are created by reading the
database.

=back

=cut

sub new {
    # Get the parameters.
    my ($class, $entityName, $loader, $stats, %options) = @_;
    # Create the object.
    my $retVal = ERDBtk::ID::new($class, $entityName, $loader, $stats, %options);
    # We need to create the prefix hash and the check-field hash.
    if ($options{hashes}) {
        # Here the caller passed them in.
        $retVal->{prefixHash} = $options{hashes}[0];
        $retVal->{checkHash} = $options{hashes}[1];
    } else {
        # Here we need to create them.
        my %prefixHash;
        my %checkHash;
        my $checkField = $retVal->{checkField};
        my $fieldList = 'id' . ($checkField ? " $checkField" : '');
        my $query = $retVal->db->Get($entityName, '', [], $fieldList);
        while (my $record = $query->Fetch()) {
            # Get the ID field and compute the next available suffix for its prefix.
            my $id = $record->PrimaryValue('id');
            UpdatePrefixHash(\%prefixHash, $id);
            # If there is a check field, put it in the check hash.
            if ($checkField) {
                my $check = $record->PrimaryValue($checkField);
                $checkHash{$check} = $id;
            }
        }
        # Store the hashes.
        $retVal->{prefixHash} = \%prefixHash;
        if ($checkField) {
            $retVal->{checkHash} = \%checkHash;
        }
    }
    # Return the object.
    return $retVal;
}


=head3 UpdatePrefixHash

    ERDBtk::ID::Magic::Exclusive::UpdatePrefixHash(\%prefixHash, $id);

Update a prefix hash with the next available suffix based on an incoming
magic name ID. The magic name is parsed, and the incoming hash is
interrogated to insure that it's specified next available suffix is truly
available.

=over 4

=item prefixHash

Reference to hash that maps each magic name prefix to the next available
suffix.

=item id

New magic name ID to be used to update the hash.

=back

=cut

sub UpdatePrefixHash {
    # Get the parameters.
    my ($prefixHash, $id) = @_;
    # The default next suffix is 2.
    my ($prefix, $suffix) = ($id, 2);
    # Check for a pre-existing suffix.
    if ($id =~ /^(.+)(\d+)$/) {
        # We want one more than the pre-existing one.
        ($prefix, $suffix) = ($1, $2 + 1);
    }
    # Store this new suffix if it's better than the old one.
    my $oldSuffix = $prefixHash->{$prefix};
    if (! defined $oldSuffix || $oldSuffix < $suffix) {
        $prefixHash->{$prefix} = $suffix;
    }
}

=head2 Public Methods

=head3 ComputeID

    my $newID = $helper->ComputeID($name);

Compute the appropriate magic name ID for an entity instance with the specified name.

=over 4

=item name

Name field value for the entity instance that needs an ID.

=item RETURN

Returns a new magic name ID appropriate to the entity instance.

=back

=cut

sub ComputeID {
    # Get the parameters.
    my ($self, $name) = @_;
    # Get the statistics and the entity name.
    my $stats = $self->stats;
    my $entityName = $self->{entityName};
    # Compute the next available ID.
    my $nameField = $self->{nameField};
    my $prefixHash = $self->{prefixHash};
    my ($prefix, $suffix) = ERDBtk::ID::Magic::Name($name);
    # Has this prefix been used before?
    my $savedSuffix = $prefixHash->{$prefix};
    if ($savedSuffix) {
        # Yes, update the suffix.
        $suffix = $savedSuffix;
        $prefixHash->{$prefix} = $suffix + 1;
        $stats->Add($entityName . "NextSuffix" => 1);
    } else {
        # No. Store the next suffix in the prefix hash. It will always be 2.
        $prefixHash->{$prefix} = 2;
        $stats->Add($entityName . "Suffix" => 1);
    }
    # Save the resulting name.
    my $retVal = $prefix . $suffix;
    # Return it.
    return $retVal;
}


=head2 Virtual Overrides

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
    my ($self, $checkValue) = @_;
    my $retVal = $self->CheckExclusive($checkValue);
    return $retVal;
}


=head3 InsertNew

    my $newID = $helper->InsertNew(%fields);

Insert a new entity instance into the database and return its ID.

=over 4

=item fields

Hash containing the field values for the new entity instance. The C<id> field
will be presumed to not have a value.

=item RETURN

Returns the ID of the entity instance after it has been inserted.

=back

=cut

sub InsertNew {
    # Get the parameters.
    my ($self, %fields) = @_;
    # Get the name field value.
    my $name = $fields{$self->{nameField}};
    # Compute the magic name ID.
    $fields{id} = $self->ComputeID($name);
    # Insert this record.
    $self->loader->InsertObject($self->{entityName}, %fields);
    # Return the new ID.
    return $fields{id};
}


1;