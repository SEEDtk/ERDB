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


package ERDBtk::Helpers::ObjectPath;

    use strict;
    use warnings;
    use base qw(ERDBtk::Helpers::SQLBuilder);
    ## more use clauses

=head1 ERDBtk Object Path Helper

This object manages an object path and can be used to perform operations on it. Currently,
it only does deletes, but in the future it may do more.

This object is based on L<ERDBtk::Helpers::SQLBuilder>. It has the following additional fields.

=over 4

=item pathList

Reference to a list of object names forming the path.

=back

=head2 Special Methods

=head3 new

    my $pathObj = ERDBtk::Helpers::ObjectPath->new($erdb, $objects);

Create a new ERDBtk Object Path Helper for a given object path.

=over 4

=item erdb

An L<ERDBtk> object for the target database.

=item objects

Reference to a list of object names forming an L<object name list|ERDBtk/Object Name List>.

=back

=cut

sub new {
    # Get the parameters.
    my ($class, $erdb, $objects) = @_;
    # Insure the object names form a list and that we're copying it so we aren't vulnerable
    # to user changes.
    my @objects;
    if (ref $objects ne 'ARRAY') {
        @objects = split(' ', $objects);
    } else {
        @objects = @$objects;
    }
    # Create the object.
    my $retVal = ERDBtk::Helpers::SQLBuilder::new($class, $erdb, $objects);
    # Add the object name list.
    $retVal->{pathList} = \@objects;
    # Return the object.
    return $retVal;
}

=head2 Query Methods

=head3 lastObject

    my ($objectName, $tableName) = $pathObj->lastObject();

Return the object name (that is, user-provided alias) and the table name
(that is, the entity or relationship name) corresponding to the last object in the path.

=cut

sub lastObject {
    # Get the parameters.
    my ($self) = @_;
    # Get the object name for the last path entry.
    my $objects = $self->{pathList};
    my $objectName = $objects->[$#{$objects}];
    # Compute its base name.
    my $tableName = $self->BaseObject($objectName);
    # Return the result.
    return ($objectName, $tableName);
}

=head3 path

    my @objects = $pathObj->path();

Return the list of object names forming this object's object name path.

=cut

sub path {
    # Get the parameters.
    my ($self) = @_;
    # Copy the path.
    my @retVal = @{$self->{pathList}};
    # Return the result.
    return @retVal;
}

=head2 Public Manipulation Methods

=head3 Delete

    my $rowCount = $pathObject->Delete($filter, \@parms);

Delete all the matching instances of the last object in the object path
list. If the last object is an embedded relationship, this method will
do nothing.

=over 4

=item filter

A L<filter clause|ERDBtk/Filter Clause> for the deletion

=item parms

Reference to a list of parameters to be filled into the parameter
marks of the filter.

=item RETURN

Returns the number of rows deleted.

=back

=cut

sub Delete {
    # Get the parameters.
    my ($self, $filter, $parms) = @_;
    # Get the ERDBtk object.
    my $erdb = $self->db;
    # Insure we have a parameter value list.
    $parms //= [];
    # Declare the return variable.
    my $retVal = 0;
    # Get the quote character.
    my $q = $self->q;
    # Get the database handle.
    my $dbh = $erdb->{_dbh};
    # Get the name and base name of the last object.
    my ($objectName, $tableName) = $self->lastObject();
    # Only proceed if the object is NOT an embedded relationship.
    if (! $erdb->IsEmbedded($tableName)) {
        # Now we need to get the list of tables to delete.
        my @secondaries = $erdb->GetSecondaryRelations($tableName);
        # Loop through the secondaries, deleting.
        for my $secondary (@secondaries) {
            # Create the delete command for the secondary.
            my ($suffix, $alias) = $self->GetSecondaryFilter($filter, $objectName, $secondary);
            my $command = "DELETE $q$alias$q $suffix";
            # Execute the delete statement.
            $retVal = $dbh->SQL($command, 0, $parms);
        }
        # Now delete the main table.
        my $suffix = $self->SetFilterClause($filter);
        my $command = "DELETE $q$objectName$q $suffix";
        # Execute the delete statement and save the row count.
        $retVal = $dbh->SQL($command, 0, @$parms);
        # Convert True Zero to a real 0.
        if ($retVal == 0) {
            $retVal = 0;
        }
    }
    # Return the result.
    return $retVal;
}


=head3 DeleteCommand

    my $command = $pathObject->Delete($filter);

Return the delete command to delete all the matching instances of the last
object in the object path list. If the last object is an embedded relationship,
this method will return C<undef>.

=over 4

=item filter

A L<filter clause|ERDBtk/Filter Clause> for the deletion.

=item RETURN

Returns the SQL statement for deleting the desired rows, or C<undef> if
the target is an embedded relationship (which never needs to be deleted).

=back

=cut

sub DeleteCommand {
    # Get the parameters.
    my ($self, $filter) = @_;
    # Get the ERDBtk object.
    my $erdb = $self->db;
    # Get the quote character.
    my $q = $self->q;
    # Declare the return variable.
    my $retVal;
    # Get the name and base name of the last object.
    my ($objectName, $tableName) = $self->lastObject();
    # Only proceed if the object is NOT an embedded relationship.
    if (! $erdb->IsEmbedded($tableName)) {
        # Form the delete statement.
        my $suffix = $self->SetFilterClause($filter);
        $retVal = "DELETE $q$objectName$q $suffix";
    }
    # Return the result.
    return $retVal;
}

1;