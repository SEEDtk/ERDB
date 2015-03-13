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


package ERDB::ID::Counter::Shared;

    use strict;
    use warnings;
    use base qw(ERDB::ID::Counter);
    use Tracer;


=head1 ERDB Auto-Counter ID Helper for Shared Database Access

This is a subclass for L<ERDB::ID> that manages auto-counter IDs for environments when the
client does not have exclusive access to the database. This means we need to check after
insertion to determine if perhaps a version of the inserted record already exists.

=head2 Special Methods

=head3 new

    my $helper = ERDB::ID::Counter::Shared->new($entityName, $loader, $stats, %options);

Construct a new ID helper object for the specified entity.

=over 4

=item entityName

Name of the entity type that this helper generates IDs for.

=item loader

A loader object used to insert records and access the database.

=item stats

A L<Stats> object used for tracking statistics.

=item options

=item options

A hash of options. The options are all interrogated by the super-classes.

=back

=cut

sub new {
    # Get the parameters.
    my ($class, $entityName, $loader, $stats, %options) = @_;
    # Create the object.
    my $retVal = ERDB::ID::new($class, $entityName, $loader, $stats, %options);
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
    my $retVal = $self->CheckShared($checkValue);
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
    # Get the ERDB object, the entity name, and the statistics object.
    my $erdb = $self->db;
    my $entityName = $self->{entityName};
    my $stats = $self->stats;
    # This will be set to TRUE when we succeed.
    my $inserted;
    while (! $inserted) {
        # Try to insert this record.
        $inserted = $erdb->InsertObject($entityName, \%fields, dup => 'ignore');
        # Did we succeed?
        if ($inserted) {
            # Yes. We are done.
            $stats->Add($entityName . "Inserted" => 1);
        } else {
            # No. We have a duplicate and need to get its ID.
            my $checkField = $self->{checkField};
            if (! $checkField) {
                Confess("InsertObject for $entityName failed, but no check field defined.");
            }
            ($retVal) = $erdb->GetFlat($entityName, "$entityName($checkField) = ?",
                    [$fields{$checkField}], 'id');
            # There is an insane possibility the duplicate was deleted while we were looking for
            # it, so we have to check for a result. If we don't find one, we'll loop.
            if ($retVal) {
                # We found the duplicate. We are done.
                $stats->Add($entityName . "DuplicateInsert" => 1);
                $inserted = 1;
            }
        }
    }
    # Return the new ID.
    return $retVal;
}


1;