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


package ERDB::ID::Magic::Shared;

    use strict;
    use warnings;
    use base qw(ERDB::ID::Magic);
    use Tracer;


=head1 ERDB Magic Name Helper for Shared Database Access

This is a subclass for L<ERDB::ID> that manages magic name IDs for environments when the
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
    # Get the statistics and database objects.
    my $stats = $self->stats;
    my $erdb = $self->db;
    # Get the entity type.
    my $entityName = $self->{entityName};
    # We will store the subsystem ID in here.
    my $retVal;
    # Do we have an ID?
    if (defined $fields{id}) {
        # Yes. Do a normal insert.
        $erdb->InsertObject($entityName, %fields);
        # Return the ID.
        $retVal = $fields{id};
    } else {
        # No. Find the name so we can compute an ID.
        my $nameField = $self->{nameField};
        my $name = $fields{$nameField};
        my ($prefix, $suffix) = ERDB::ID::Magic::Name($name);
        # Look for examples of this ID.
        my ($id) = $erdb->GetFlat($entityName, "$entityName(id) LIKE ? ORDER BY $entityName(id) DESC LIMIT 1", ["$prefix%"], 'id');
        # Was a version of this ID found?
        if ($id) {
            # Yes. Try to compute a suffix.
            if ($id eq $prefix) {
                # Here we've found the exact same ID. Use a suffix of 2 to distinguish us.
                $suffix = 2;
            } elsif (substr($id, length($prefix)) =~ /^(\d+)$/) {
                # Here we've found the same ID with a numeric suffix. Compute a new suffix that is 1 greater.
                $suffix = $1 + 1;
            }
        }
        # Try to insert, incrementing the suffix until we succeed.
        my $okFlag;
        while (! $okFlag) {
            $retVal = $prefix . $suffix;
            # In case the caller did "id => undef" to denote we have no ID, we need to put the ID value directly in
            # the field hash, overriding the old value.
            $fields{id} = $retVal;
            $okFlag = $erdb->InsertObject($entityName, \%fields, dup => 'ignore');
            # Increment the suffix. Note we go from empty string to 2. There is no "1" suffix unless the prefix ended with a digit.
            $suffix = ($suffix ? $suffix + 1 : 2);
        }
    }
    # Return the entity instance ID.
    return $retVal;
}


1;