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


package ERDB::Helpers::SQLBuilder;

    use strict;
    use warnings;


=head1 ERDB SQL Statement Builder

This object is used to manage the creation of SQL statements for ERDB. An SQL statement must be
formed from an L<object name list|ERDB/Object Name List>, a L<filter clause|ERDB/FIlter Clause>,
and a L<list of field names|ERDB/Standard Field Name Format>. The lists can be either list
references or space-delimited strings.

This object must turn these three pieces of information into an SQL statement suffix, an SQL
field list, and instructions for retrieving the fields returned by the query. The most complex
part of this is processing the object name list. It is this list that generates the join
clauses and the from-list.

This object has the following fields.

=over 4

=item ERDB

The L<ERDB> object for which an SQL statement is being built.

=item fromList

Reference to a list of FROM-clause specifiers. Each specifier consists of a table name followed by a
space and an alias.

=item aliasMap

Reference to a hash mapping each of the user's object names to a 2-tuple consisting of (0) the object's
real entity or relationship name in the database and (1) the alias assigned to it in the from-list.

=item joinWhere

Reference to a list of join clause components.

=item primary

The object name of the primary (first) object in the object name list. This is the default
object for field names.

=item fieldMap

A hash mapping the name of each retrieved field (as the user would address it) to its
position in the return from the query.

=item secondaries

Reference to a hash containing the secondary relations currently incorporated into the
query's FROM and WHERE clauses. Each secondary relation maps to the attached objectname.

=item q

The quote character for the relevant DBMS.

=back

=head2 Special Methods

=head3 new

    my $sqlHelper = ERDB::Helpers::SQLBuilder->new($erdb, $objectNames);

Create a new, blank SQL helper object for a specified object name list.

=over 4

=item erdb

The L<ERDB> object for the database this SQL statement will access.

=item objectNames

An ERDB L<object name list|ERDB/Object Name List>, specified as either a list
reference or a space-delimited string.

=back

=cut

sub new {
    # Get the parameters.
    my ($class, $erdb, $objectNames) = @_;
    # Get the data structures we're building in this method.
    my @fromList;
    my %aliasMap;
    my @joinWhere;
    # Initialize our object.
    my $retVal = { ERDB => $erdb,
        fromList => \@fromList,
        aliasMap => \%aliasMap,
        joinWhere => \@joinWhere,
        fieldMap => {},
        secondaries => {}
    };
    # Get the quote character.
    my $q = $erdb->{_quote};
    $retVal->{q} = $q;
    # Bless the object.
    bless $retVal, $class;
    # This hash will track the next available suffix number for generated
    # object names. We generate such names for embedded relationships and
    # tables we add to implement direct crossings.
    my %suffixes;
    # Convert the object name list to an array.
    if (ref $objectNames ne 'ARRAY') {
        # This construct splits the list on \s+ but trims leading spaces first.
        $objectNames = split ' ', $objectNames;
    }
    # Save the first object name as the primary.
    $retVal->{primary} = $objectNames->[0];
    # Now we need to run through the object name list. For each object name,
    # we compute the table name, the base name,  and whether or not it is embedded.
    # An AND is converted to an object name of an empty string.
    my $AND = ['', 0, '', ''];
    my @parsedObjects;
    for my $objectName (@$objectNames) {
        # Is this the special case of an 'AND'?
        if (uc $objectName eq 'AND') {
            # Yes. Blank it and continue.
            push @parsedObjects, $AND;
        } else {
            # Now we need to compute the object's alias name
            # and its table name. Parse the object name.
            my $baseName = $objectName;
            if ($objectName =~ /(.+?)\d+$/) {
                $baseName = $1;
            }
            my ($tableName, $embedFlag) = $erdb->_AnalyzeObjectName($baseName);
            push @parsedObjects, [$objectName, $embedFlag, $tableName, $baseName];
        }
    }
    # Start with a blank.
    my $prevObject = $AND;
    # Now we loop through, creating the alias map, the join list, and the
    # from list.
    for my $objectData (@parsedObjects) {
        my ($objectName, $embedFlag, $tableName, $baseName) = @$objectData;
        # Is this a real object?
        if ($objectName ne '') {
            # Yes. Get the previous object's fields.
            my ($prevName, $prevEmbed, $prevTable, $prevBase) = @$prevObject;
            # Try to get the new object's alias data.
            my $aliasData = $aliasMap{$objectName};
            # If we've never seen this object before, we need to compute the
            # alias stuff.
            if (! $aliasData) {
                # Check to see if we are sharing a table with the previous object.
                if ($embedFlag) {
                    if ($tableName eq $prevTable) {
                        # We are embedded in the previous object. Use its alias.
                        $aliasData = $aliasMap{$prevName};
                    } else {
                        # Here we must generate an alias for the target table.
                        my $suffix = $suffixes{$tableName} // 100;
                        $suffixes{$tableName} = $suffix + 1;
                        my $aliasName = "$tableName$suffix";
                        $aliasData = [$baseName, $aliasName];
                        # Add the alias to the FROM list.
                        $retVal->UpdateFrom($tableName, $aliasName);
                    }
                } elsif ($prevObject->[1] && $tableName eq $prevObject->[0]) {
                    # The previous object is embedded in us. Use its alias.
                    $aliasData = $aliasMap{$prevName};
                } else {
                    # There is no embedding. Use the object name as the alias.
                    $aliasData = [$baseName, $objectName];
                    # Add it to the FROM list.
                    $retVal->UpdateFrom($tableName, $objectName);
                }
                # Save the alias data in the alias map.
                $aliasMap{$objectName} = $aliasData;
            }
            # Do we need a join?
            if ($prevName ne '' && $aliasMap{$prevName}[1] ne $aliasData->[1]) {
                # Yes. There was a previous object, and it represents a different table
                # instance. We need to generate a join. Get the join instructions.
                my $joinList = $erdb->_GetCrossing($prevBase, $baseName);
                if (! defined $joinList) {
                    Confess("No path available from $prevTable to $tableName.");
                } elsif (@$joinList) {
                    # Here we have join instructions to loop through. Get a list of
                    # the available objects. Note that they must be different. If
                    # an object crosses to itself the join list is empty. Most objects
                    # don't cross to themselves.
                    my %objects = ($prevTable => $aliasMap{$prevName}[1],
                            $tableName => $aliasData->[1]);
                    for my $join (@$joinList) {
                        my ($tab1, $field1, $tab2, $field2) = @$join;
                        push @joinWhere, "$q$objects{$tab1}$q.$q$field1$q = $q$objects{$tab2}$q.$q$field2$q";
                    }
                }
            }
        }
        # Set up for the next iteration.
        $prevObject = $objectData;
    }
    # Return this object.
    return $retVal;
}

=head2 Manipulation Methods

=head3 ComputeFieldList

    my $fieldList = $sqlHelper->ComputeFieldList($fields);

Compute the field list string for the current query. The resulting string
can be plugged into a SELECT statement and will return results that can
be found by name using a hash built internally in this object.

=over 4

=item fields

A field name list in L<standard field name format|ERDB/Standard Field Name Format>. If undefined,
then all fields in the various objects will be returned. If an empty string, no fields
will be returned.

=item RETURN

Return the field list string for the SELECT statement.

=back

=cut

sub ComputeFieldList {
    # Get the parameters.
    my ($self, $fields) = @_;
    # Declare the return variable. We will assemble the pieces in here and then join them
    # into a string at the end.
    my @retVal;
    # Get the ERDB object.
    my $erdb = $self->{ERDB};
    # Get the alias map.
    my $aliasMap = $self->{aliasMap};
    # Get the field map. We fill it in here.
    my $fieldMap = $self->{fieldMap};
    # This will contain the names of objects that require the presence of IDs.
    my %secondaryBases;
    # Get the list of field names. If it is a string, convert it into an array. If it is undefined, get all the
    # fields.
    my @fields;
    if (! defined $fields) {
        # Here we want every field in every table. Loop through the object names.
        for my $objectName (keys %$aliasMap) {
            # Get this object's base name.
            my $baseName = $aliasMap->{$objectName}[0];
            # Get the object's field data.
            my $fields = $erdb->GetFieldTable($baseName);
            # Loop through the fields.
            for my $field (keys %$fields) {
                # Add this field to the field list.
                push @fields, "$objectName($field->{name})";
            }
        }
    } elsif (ref $fields eq 'ARRAY') {
        # Here we already have a list.
        push @fields, @$fields;
    } else {
        # Here we need to split a string.
        push @fields, split(' ', $fields);
    }
    # Now we have a field list with all our fields in it. Loop through the list.
    for my $field (@fields) {
        # Parse the field name.
        my ($objectName, $fieldName) = $self->ParsedFieldName($field);
        # Get the base name of the object.
        my $baseName = $self->BaseObject($objectName);
        # Is this a secondary field?
        if ($erdb->IsSecondary("$baseName($fieldName)")) {
            # Yes. Save its object name. We don't get it using the
            # query, but we will need its object name's ID.
            $secondaryBases{$objectName} = 1;
        } else {
            # Here we have a primary field. This goes in the query.
            # Get the field's real name.
            my $sqlName = $self->FixName($field);
            # Remember its location.
            $fieldMap->{"$objectName($fieldName)"} = scalar @retVal;
            # Push it into the output string.
            push @retVal, $sqlName;
        }
    }
    # Make sure we have IDs for all the objects with secondaries.
    for my $objectName (keys %secondaryBases) {
        my $objectID = "$objectName(id)";
        if (! defined $fieldMap->{"$objectID"}) {
            # Get the field's real name.
            my $sqlName = $self->FixName($objectID);
            # Remember its location.
            $fieldMap->{"$sqlName"} = scalar @retVal;
            # Push it into the output string.
            push @retVal, $sqlName;
        }
    }
    # Return the result.
    return join(", ", @retVal);
}


=head3 SetFilterClause

    my $suffix = $sqlHelper->SetFilterClause($filterClause);

This method computes the SQL statement suffix from the filter clause.

=over 4

=item filterClause

An L<ERDB filter clause|ERDB/Filter Clause> constraining the current query.

=item RETURN

Returns the SQL statement suffix, including the FROM and WHERE clauses.

=back

=cut

sub SetFilterClause {
    # Get the parameters.
    my ($self, $filterClause) = @_;
    # Get a copy of the filter clause.
    my $filter = $filterClause;
    # Get the alias map.
    my $aliasMap = $self->{aliasMap};
    # Loop through the object names.
    for my $objectName (keys %$aliasMap) {
        # Get the object's base name.
        my $baseName = $self->BaseObject($objectName);
        # Fix up all occurrences of this object name.
        $filter =~ s/$objectName\(([a-zA-Z\-]+)\)/$self->FixNameWithSecondaryCheck($objectName, $1, $baseName)/sge;
    }
    # Assemble the filter clause and the FROM data into a suffix.
    my $retVal = "FROM " . join(", ", @{$self->{fromList}});
    if ($filter) {
        # Here we have a filter. If it contains a constraint, tack on WHERE.
        unless ($filter =~ /^(?:LIMIT\s+\d|ORDER\s+BY\s+)/i) {
            $filter .= "WHERE $filter";
        }
        # Add the filter to the suffix.
        $retVal .= " $filter";
    }
    # Return the result.
    return $retVal;
}

=head3 UpdateFrom

    $sqlHelper->UpdateFrom($tableName, $alias);

Add a new object to the from-list.

=over 4

=item table

Table name of the object to add.

=item alias

Name to give the object added.

=back

=cut

sub UpdateFrom {
    # Get the parameters.
    my ($self, $tableName, $alias) = @_;
    # Get the quote character.
    my $q = $self->{q};
    # Form the from-segment in here.
    my $from = "$q$tableName$q";
    if ($tableName ne $alias) {
        # Here we need to state the alias.
        $from .= " $q$alias$q";
    }
    # Push the from-segment onto the from-list.
    push @{$self->{fromList}}, $from;
}


=head2 Query Methods

=head3 db

    my $erdb = $sqlHelper->db;

Return the relevant L<ERDB> object.

=cut

sub db {
    return $_[0]->{ERDB};
}

=head3 ParseFieldName

    my ($objectName, $fieldName) = $sqlHelper->ParseFieldName($name)'

This method will take a field name in L<standard field name format|ERDB/Standard Field Name Format>
and separate out the object name and field name parts. If no object name is present, the primary
object name is used.

=over 4

=item name

The field name to parse.

=item RETURN

Returns a 2-element list consisting of (0) the object name containing the field and (1) the name
of the field itself.

=back

=cut

sub ParseFieldName {
    # Get the parameters.
    my ($self, $name) = @_;
    # The return values will go in here.
    my ($objectName, $fieldName);
    if ($name =~ /^([^(]+)\(([^)])\)/) {
        # Here an object name was present.
        ($objectName, $fieldName) = ($1, $2);
    } else {
        # Here we're using the primary object.
        ($objectName, $fieldName) = ($self->{primary}, $name);
    }
    # Return the parsed values.
    return ($objectName, $fieldName);
}


=head3 PrimaryInfo

    my ($objectName, $baseName) = $sqlHelper->PrimaryInfo();

Return the object name and the base name for the primary object. The object name is what
the object is called by the user; the base name is the name of the underlying entity or
relationship.

=cut

sub PrimaryInfo {
    # Get the parameters.
    my ($self) = @_;
    # Get the alias map.
    my $aliasMap = $self->{aliasMap};
    # Get the primary object name.
    my $objectName = $self->{primary};
    # Extract its base name.
    my $baseName = $aliasMap->{$objectName}[0];
    # Return the data found.
    return ($objectName, $baseName);
}


=head3 FixName

    my $fixedName = $sqlBuilder->FixName($objectName, $fieldName);

Create a reference to the specified field. The object name is translated to its alias
and the field name is converted to its SQL form.

=over 4

=item objectName

An object name from the current query path.

=item fieldName

A field name from that object.

=item RETURN

Returns the SQL reference to the field, in the form of the alias name, a period (C<.>)),
and the SQL version of the field name.

=back

=cut

sub FixName {
    # Get the parameters.
    my ($self, $objectName, $fieldName) = @_;
    # Get the ERDB object.
    my $erdb = $self->{ERDB};
    # Get the quote character.
    my $q = $self->{q};
    # Compute the alias name-- that is, the name assigned to the object in the FROM clause--
    # and the table name.
    my $aliasData = $self->{aliasMap}{$objectName};
    if (! $aliasData) {
        Confess("$objectName not found in this query.");
    }
    my ($baseName, $aliasName) = @$aliasData;
    # Find the real field name.
    my $sqlName = $self->{ERDB}->_SQLFieldName($baseName, $fieldName);
    if (! $sqlName) {
        Confess("Field $fieldName not found in object $objectName.");
    }
    # Return the SQL.
    my $retVal = "$q$aliasName$q.$q$sqlName$q";
    return $retVal;
}


=head3 FixNameWithSecondaryCheck

    my $fixedName = $sqlBuilder->FixNameWithSecondaryCheck($objectName, $fieldName, $baseName);

This is a version of L</FixName> that serves the special needs of parsing a filter
clause. The name is converted to SQL format, but if it is a secondary field, then
we will insure the secondary relation is incorporated into the from-list and join clause.

=over 4

=item objectName

An object name from the current query path.

=item fieldName

A field name from that object.

=item baseName

The name of the object in the database that the specified object belongs to.

=item RETURN

Returns the SQL reference to the field, in the form of the alias name, a period (C<.>)),
and the SQL version of the field name.

=back

=cut

sub FixNameWithSecondaryCheck {
    # Get the parameters.
    my ($self, $objectName, $fieldName, $baseName) = @_;
    # Get the ERDB object.
    my $erdb = $self->{ERDB};
    # Get the quote character,
    my $q = $self->{q};
    # Fix the name.
    my $retVal = $self->FixName($objectName, $fieldName);
    # Is this a secondary field?
    if ($erdb->IsSecondary("$baseName($fieldName)")) {
        # Yes. Get the secondary relation's name.
        my $secondaryName = $erdb->GetFieldRelationName($baseName, $fieldName);
        # Compute its object name. It has the same suffix as the incoming name.
        my $suffix = '';
        if ($objectName =~ /(\d+)$/) {
            $suffix = $1;
        }
        my $secondaryAlias = "$secondaryName$suffix";
        # Is it already in the database?
        if (! $self->{secondaries}{$secondaryAlias}) {
            # No. Add a from-clause.
            $self->UpdateFrom($secondaryName, $secondaryAlias);
            # Add a join clause.
            my $qid = $q . 'id' . $q;
            my $join = "$q$objectName$q.$qid = $q$secondaryAlias$q.$qid";
            push @{$self->{joinWhere}}, $join;
            # Insure we know that we have this relation.
            $self->{secondaries}{$secondaryAlias} = $objectName;
        }
    }
    # Return the fixed name.
    return $retVal;
}


=head3 BaseObject

    my $baseName = $sqlHelper->BaseObject($objectName);

Return the base object corresponding to the specified object name.

=over 4

=item objectName

Object name whose base name is desired.

=item RETURN

Returns the name of the entity or relationship represented by the object.
If the object is not in this query, it returns C<undef>.

=back

=cut

sub BaseObject {
    # Get the parameters.
    my ($self, $objectName) = @_;
    # The return value will be put in here.
    my $retVal;
    # Check the alias map for this object.
    my $aliasData = $self->{aliasMap}{$objectName};
    # Only proceed if we're in the map.
    if ($aliasData) {
        # Return the base name.
        $retVal = $aliasData->[0];
    }
    # Return the result.
    return $retVal;
}

=head3 GetFieldNames

    my \@fieldNames = $sqlBuilder->GetFieldNames();

Return a complete list of the available field names for this query.

=cut

sub GetFieldNames {
    # Get the parameters.
    my ($self) = @_;
    # Return the keys of the field name hash.
    my $retVal = [sort keys %{$self->{fieldMap}}];
    return $retVal;
}


=head3 GetFieldIndex

    my $index = $sqlBuilder->GetFieldIndex($name);

Return the index in the query result set of the field represented by a field name,
ot C<undef> if the field is not present.

=over 4

=item name

The name, in L<standard field name format|ERDB/Standard Field Name Format>, of the
desired field.

=item RETURN

Returns the index number (0-based) of the desired field, or C<undef> if the field
is not present.

=back

=cut

sub GetFieldIndex {
    # Get the parameters.
    my ($self, $name) = @_;
    # Parse the field name.
    my ($objectName, $fieldName) = $self->ParseFieldName($name);
    # Look for it in the field mape.
    my $retVal = $self->{fieldMap}{"$objectName($fieldName)"};
    # Return the result.
    return $retVal;
}


1;