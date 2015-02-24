package ERDB::Object;

    use strict;
    use DBKernel;
    use Tracer;

=head1 Entity-Relationship Database Package Instance Object

=head2 Introduction

This package defines the instance object for the Entity-Relationship Database
Package (L<ERDB>. This object is usually returned by the C<Fetch> method of
the B<ERDB::Query> object.

An instance object allows the user to access the fields in the current instance.
The instance consists of zero or more entity and/or relationship objects and a
map of field names to locations. Some entity fields require additional queries
to the database. If the entity object is present, the additional queries are
executed automatically. Otherwise, the value is treated as missing.

Each L<ERDB::Object> has at least one object called the I<target object>. This
is generally the first named object in the query that created this object.
The target object name is used as the default when parsing field names.

=head2 Public Methods

=head3 Attributes

    my $attrNames = $dbObject->Attributes();

This method will return a sorted list of the attributes present in this object.
The list can be used in the L</Values> method to get all the values stored.

If the ERDB::Object was created by a database query, the attributes returned will
only be those which occur on primary relations. Additional fields may get
loaded into the object if the client has asked for them in a L</Value> or
L</Values> command. Initially, however, only the primary fields-- each of which
has one and only one value-- will be found in the attribute list.

=cut
#: Return Type @;
sub Attributes {
    # Get the parameters.
    my ($self) = @_;
    # Get the keys of the value hash.
    my $retVal = $self->{_helper}->GetFieldNames();
    # Return the result.
    return $retVal;
}

=head3 HasField

    my $flag = $dbObject->HasField($fieldSpec);

Return TRUE if this object has the specified field available, else FALSE.
This method can be used to determine if a value is available without
requiring an additional database query.

=over 4

=item fieldSpec

A standard field specifier. See L<ERDB/Standard Field Name Format>. The
default table name is the object's target entity.

=item RETURN

Returns TRUE if there's a value for the field in this object, else FALSE.

=back

=cut

sub HasField {
    # Get the parameters.
    my ($self, $fieldName) = @_;
    # Ask the helper about this field.
    my $fieldIndex = $self->{_helper}->GetFieldIndex($fieldName);
    # Return the result.
    return (defined $fieldIndex ? 1 : 0);
}


=head3 PrimaryValue

    my $value = $dbObject->PrimaryValue($name);

Return the primary value of a field. This will be its first value in a standard
value list.

This method is a more convenient version of L</Value>. Basically, the call

    my ($value) = $dbObject->Value($name);

is equivalent to

    my $value = $dbObject->PrimaryValue($name);

but the latter is syntactically more convenient.

=over 4

=item name

Name of the field whose value is desired, in the L<ERDB/Standard Field Name Format>.

=item RETURN

Returns the value of the specified field, or C<undef> if the field has no value.

=back

=cut

sub PrimaryValue {
    # Get the parameters.
    my ($self, $name) = @_;
    # Get the value.
    my ($retVal) = $self->Value($name);
    # Return it.
    return $retVal;
}


=head3 Value

    my @values = $dbObject->Value($attributeName, $rawFlag);

Return a list of the values for the specified attribute.

=over 4

=item attributeName

Name of the desired attribute, in the L<ERDB/Standard Field Name Format>.

=item rawFlag (optional)

If TRUE, then the data will be returned in raw form, without decoding from the
database format.

=item RETURN

Returns a list of the values for the specified attribute.

=back

=cut

sub Value {
    # Get the parameters.
    my ($self, $attributeName, $rawFlag) = @_;
    # Get the database and the helper object.
    my $helper = $self->{_helper};
    my $erdb = $helper->db;
    # Get the quote character.
    my $q = $erdb->q;
    # Declare the return variable.
    my @retVal = ();
    # Parse the field name.
    my ($objectName, $fieldName) = $helper->ParseFieldName($attributeName);
    # Compute a normalized name for the field.
    my $baseName = $helper->BaseObject($objectName);
    my $normalName = "$baseName($fieldName)";
    # Do we already have the field?
    my $fieldIndex = $helper->GetFieldIndex($attributeName);
    if (defined $fieldIndex) {
        # Yes. Get the value.
        my $value = $self->{_row}[$fieldIndex];
        # Decode and return it.
        push @retVal, $erdb->DecodeField($normalName, $value);
    } else {
        # Determine if we have a secondary field.
        if (! $erdb->IsSecondary($normalName)) {
            # We do not.
            Confess("Invalid field name \"$fieldName\".");
        } else {
            # We must first find the field's data structure.
            # If the field name is invalid, this will throw an error.
            my $fieldData = $erdb->_FindField($attributeName, $objectName);
            # Insure we have an ID for this entity.
            my $idIdx = $helper->GetFieldIndex("$objectName(id)");
            if (! defined($idIdx)) {
                Confess("Cannot retrieve field \"$attributeName\": it is not part of this query.");
            } else {
                # Get the ID value.
                my $idValue = $self->{_row}[$idIdx];
                # Determine the name of the relation that contains this field.
                my $relationName = $fieldData->{relation};
                # Compute the actual name of the field in the database.
                my $fixedFieldName = $fieldData->{realName};
                # Create the SELECT statement for the desired relation and execute it.
                my $command = "SELECT $q$fixedFieldName$q FROM $q$relationName$q WHERE id = ?";
                my $sth = $erdb->_GetStatementHandle($command, [$idValue]);
                # Loop through the query results creating a list of the values found.
                my $rows = $sth->fetchall_arrayref;
                for my $row (@{$rows}) {
                    # Are we decoding?
                    if ($rawFlag) {
                        # No, stuff the value in the result list unmodified.
                        push @retVal, $row->[0];
                    } else {
                        # Yes, decode it before stuffing.
                        push @retVal, $erdb->DecodeField($normalName, $row->[0])
                    }
                }
            }
        }
    }
    # Return the field values found.
    return @retVal;
}


=head3 Values

    my @values = $dbObject->Values(\@attributeNames);

This method returns a list of all the values for a list of field specifiers.
Essentially, it calls the L</Value> method for each element in the parameter
list and returns a flattened list of all the results.

For example, let us say that C<$feature> contains a feature with two links and a
translation. The following call will put the feature links in C<$link1> and
C<$link2> and the translation in C<$translation>.

    my ($link1, $link2, $translation) = $feature->Values(['Feature(link)', 'Feature(translation)']);

=over 4

=item attributeNames

Reference to a list of attribute names, or a space-delimited string of attribute names.

=item RETURN

Returns a flattened list of all the results found for each specified field.

=back

=cut

sub Values {
    # Get the parameters.
    my ($self, $attributeNames) = @_;
    # Create the return list.
    my @retVal = ();
    # Create the attribute name list.
    my @attributes;
    if (ref $attributeNames eq 'ARRAY') {
        @attributes = @$attributeNames;
    } else {
        @attributes = split ' ', $attributeNames;
    }
    # Loop through the specifiers, pushing their values into the return list.
    for my $specifier (@attributes) {
        push @retVal, $self->Value($specifier);
    }
    # Return the resulting list.
    return @retVal;
}

=head2 Internal Methods

=head3 _new

    my $erdbObject = ERDB::Object->_new($helper, \@values);

Create an B<ERDB::Object> for the current database row.

=over 4

=item helper

L<ERDB::Helpers::SQLBuilder> object for the relevant query.

=item values

Reference to a list of the values returned for the current result row.

=item RETURN

Returns an B<ERDB::Object> that can be used to access fields from this row of data.

=back

=cut

sub _new {
    # Get the parameters.
    my ($class, $helper, $values) = @_;
    # Create this object.
    my $self = {
        _helper => $helper,
        _row => $values
    };
    # Bless and return it.
    bless $self, $class;
    return $self;
}


1;
