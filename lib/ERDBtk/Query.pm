package ERDBtk::Query;

    use strict;
    use ERDBtk::Object;
    use StringUtils;

=head1 Entity-Relationship Database Package Query Iterator

=head2 Introduction

This package defines the Iteration object for an Entity-Relationship Database. The iteration object
represents a filtered SELECT statement against an Entity-Relationship Database, and provides
methods for getting the appropriate records.

There are two common ways an iteration object can be created. An I<entity iterator> is created when the
client asks for objects of a given entity type. A I<relationship iterator> is created when the
client asks for objects across a relationship starting from a specific entity instance. The
entity iterator returns a single object at each position; the relationship iterator returns two
objects at each position-- one for the target entity, and one for the relationship instance
that connects it to the source entity.

For example, a client could ask for all B<Feature> instances that are marked active. This would
return an entity iterator. Each position in the iteration would consist of a single
B<Feature> instance. From a specific B<Feature> instance, the client could decide to cross the
B<IsLocatedIn> relationship to get all the B<Contig> instances which contain residues that
participate in the feature. This would return a relationship iterator. Each position in the
iterator would contain a single B<IsLocatedIn> instance and a single B<Contig> instance.

At each point in the result set, the iterator returns a B<ERDBtk::Object>. The ERDBtk::Object allows the
client to access the fields of the current entity or relationship instance.

It is also possible to ask for many different objects in a single iterator by chaining long
sequences of entities together by relationships. This is discussed in the documentation for the
B<ERDBtk> object's C<Get> method.

Finally, objects of this type should never by created directly. Instead, they are created
by the aforementioned C<Get> method and its variants.

This object has the following fields.

=over 4

=item _db

The L<ERDBtk> object for the relevant database.

=item _sth

The L<DBtk> statement handle.

=item _helper

The L<ERDBtk::Helpers::SQLBuilder> object describing the query components.

=item _results

A counter of the number of records read.

=head2 Public Methods

=head3 Fetch

    my $dbObject = $dbQuery->Fetch();

Retrieve a record from this query. The record returned will be a B<ERDBtk::Object>, which
may represent a single entity instance or a list of entity instances joined by relationships.
The first time this method is called it will return the first result from query. After that it
will continue sequentially. It will return an undefined value if we've reached the end of the
result set.

=cut

use constant FROMTO => { 'from-link' => 'to-link', 'to-link' => 'from-link' };

sub Fetch {
    # Get the parameters;
    my ($self) = @_;
    # Declare the return variable.
    my $retVal;
    # Get the statement handle.
    my $sth = $self->{_sth};
    # Fetch the next row in the query result set.
    my @row = $sth->fetchrow;
    # Check to see if we got any results.
    if (@row == 0) {
        # Here we have no result. If we're at the end of the result set, this is
        # okay, because we'll be returning an undefined value in $retVal. If an
        # error occurred, we need to abort.
        if ($sth->err) {
            # Get the error message from the DBtk object.
            my $dbh = $self->{_db}->{_dbh};
            my $msg = $dbh->ErrorMessage($sth);
            # Throw an error with it.
            Confess($msg);
        }
    } else {
        # Here we have a result, so we need to turn it into an instance object.
        $retVal = ERDBtk::Object->_new($self->{_helper}, \@row);
        # Count this result.
        $self->{_results}++;
    }
    # Return the result.
    return $retVal;
}

=head3 DefaultObjectName

    my $objectName = $query->DefaultObjectName();

Return the name of this query's default entity or relationship.

=cut

sub DefaultObjectName {
    # Get the parameters.
    my ($self) = @_;
    # The helper object knows this.
    my ($retVal) = $self->{_helper}->PrimaryInfo();
    # Return the result.
    return $retVal;
}


=head3 CheckFieldName

    my ($objectName, $fieldName, $type) = $query->CheckFieldName($name);

Analyze a field name (such as might be found in a L<ERDBtk/GetAll>.
parameter list) and return the real name of the relevant entity or
relationship, the field name itself, and the associated type object
(which will be a subclass of L<ERDBtk::Type>. If the field name is invalid, one
or more of the three results will be undefined.

=over 4

=item name

Field name to examine, in the L<standard field name format|ERDBtk/Standard Field Name Format>.

=item RETURN

Returns a 3-tuple containing the name of the object containing the field, the
base field name, and a type object describing the field's type. If the field
descriptor is invalid, the returned object name will be undefined. If the object
name is invalid, the returned field name will be undefined, and if the field
name is invalid, the returned type will be undefined.

=back

=cut

sub CheckFieldName {
    # Get the parameters.
    my ($self, $name) = @_;
    # Declare the return variables.
    my ($objectName, $fieldName, $type);
    # Get the helper object.
    my $helper = $self->{_helper};
    # Get the ERDBtk object.
    my $erdb = $self->{_db};
    # Only proceed if the field name format is valid.
    if ($name =~ /^(?:\w+\([\w\-]+\))|[\w-]+$/) {
        # Get the object and field names.
        ($objectName, $fieldName) = $helper->ParseFieldName($name);
        # Get the base name.
        my $baseName = $helper->BaseObject($objectName);
        if (! defined $baseName) {
            # The object was not found, so blank the field name.
            $fieldName = undef;
        } else {
            # Try to find the field's descriptor.
            my $objectData = $erdb->_GetStructure($baseName);
            my $fieldData = $objectData->{Fields}{$fieldName};
            # Only proceed if we found the field.
            if ($fieldData) {
                # Get the data types.
                my $typeHash = ERDBtk::GetDataTypes();
                # Get the field's type.
                $type = $typeHash->{$fieldData->{type}};
            }
        }
    }
    # Return the results.
    return ($objectName, $fieldName, $type);
}


=head2 Internal Methods

=head3 _new

    my $query = ERDBtk::Query->new($database, $sth, $sqlHelper);

Create a new query object.

=over 4

=item database

L<ERDBtk? object for the relevant database.

=item sth

Statement handle for the SELECT clause generated by the query.

=item sqlHelper

L<ERDBtk::Helpers::SQLBuilder> object used to build this query.
=back

=cut

sub _new {
    # Get the parameters.
    my ($database, $sth, $sqlHelper) = @_;
    # Create this object.
    my $self = { _db => $database, _sth => $sth, _helper => $sqlHelper,
                 _results => 0 };
    # Bless and return it.
    bless $self;
    return $self;
}


1;
