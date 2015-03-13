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


package ERDB::ID::Counter::Exclusive;

    use strict;
    use warnings;
    use base qw(ERDB::ID::Counter);


=head1 ERDB Auto-Counter ID Helper for Exclusive Database Access

This is a subclass for L<ERDB::ID> that manages auto-counter IDs for environments when the
client has exclusive access to the database. This allows a great deal of optimization, since
once we load things into memory we don't have to worry about the information becoming obsolete.


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
    # If there is a check field, create a check-field hash.
    my $checkField = $options{checkField};
    if ($checkField) {
        my %checkHash = map { $_->[1] => $_->[0] } $retVal->db->GetAll($entityName, '', [], "id $checkField");
        $stats->Add($entityName . "CheckHash" => scalar keys %checkHash);
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
    # Compute the next available ID.
    my $retVal = $self->NextID;
    $fields{id} = $retVal;
    # Insert this record.
    $self->loader->InsertObject($self->{entityName}, %fields);
    # Return the new ID.
    return $retVal;
}


1;