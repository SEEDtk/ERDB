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


package ERDB::ID::Magic::Exclusive;

    use strict;
    use warnings;
    use base qw(ERDB::ID::Magic);


=head1 ERDB Auto-Counter ID Helper for Exclusive Database Access

This is a subclass for L<ERDB::ID> that manages auto-counter IDs for environments when the
client has exclusive access to the database. This allows a great deal of optimization, since
once we load things into memory we don't have to worry about the information becoming obsolete.

In addition to the fields in the super-classes, this object contains the following fields.

=over 4

=item prefixHash

For each magic name prefix, the number of the next available suffix.

=back

=head2 Special Methods

=head3 new

    my $helper = ERDB::ID::Counter::Exclusive->new($entityName, $loader, $stats, %options);

Construct a new ID helper object for the specified entity.

=over 4

=item entityName

Name of the entity type that this helper generates IDs for.

=item loader

A loader object used to insert records and access the database.

=item stats

A L<Stats> object used for tracking statistics.

=item options

A hash of options. The options are all interrogated by the super-classes.

=back

=cut

sub new {
    # Get the parameters.
    my ($class, $entityName, $loader, $stats, %options) = @_;
    # Create the object.
    my $retVal = ERDB::ID::new($class, $entityName, $loader, $stats, %options);
    # We need to create the prefix hash and the check-field hash.
    my %prefixHash;
    my %checkHash;
    my $checkField = $retVal->{checkField};
    my $fieldList = 'id' . ($checkField ? " $checkField" : '');
    my $query = $retVal->db->Get($entityName, '', [], $fieldList);
    while (my $record = $query->Fetch()) {
        # Get the ID field and compute the next available suffix for its prefix.
        my $id = $record->PrimaryValue('id');
        if ($id =~ /^(.+?)(\d+)$/) {
            $prefixHash{$1} = $2 + 1;
        } else {
            $prefixHash{$id} = 2;
        }
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
    # Return the object.
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
    # Get the statistics and the entity name.
    my $stats = $self->stats;
    my $entityName = $self->{entityName};
    # Compute the next available ID.
    my $nameField = $self->{nameField};
    my $prefixHash = $self->{prefixHash};
    my ($prefix, $suffix) = ERDB::ID::Magic::Name($fields{$nameField});
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
    $fields{id} = $retVal;
    # Insert this record.
    $self->loader->InsertObject($entityName, %fields);
    # Return the new ID.
    return $retVal;
}


1;