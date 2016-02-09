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


package ERDBtk::Helpers::Scripts;

    use strict;
    use warnings;

=head1 Database Script Helpers

This package contains utilities for helping with database scripts.

=head2 Public Methods

=head3 field_list

    my @fieldRows = ERDBtk::Helpers::Scripts::field_list($name, $objectData);

Return a list of 2-tuples describing the fields in the specified entity
or relationship. Each 2-tuple will consist of (0) a name and (1) a data
type.

=over 4

=item name

Name of the entity or relationship.

=item objectData

An entity or relationship descriptor from the L<ERDBtk> object.

=item RETURN

Returns a list of 2-tuples describing the fields in the specified entity or relationship.

=back

=cut

sub field_list {
    # Get the parameters.
    my ($name, $objectData) = @_;
    # Loop through the fields. We store the fields in the following hash, keyed by the sort value.
    my $fieldH = $objectData->{Fields};
    my %tuples;
    for my $fname (sort keys %$fieldH) {
        my $data = $fieldH->{$fname};
        # Only include native fields.
        if (! $data->{imported}) {
            my $sorter = $data->{PrettySort};
            my $type = $data->{type};
            if ($name ne $data->{relation}) {
                $type .= " array";
            }
            push @{$tuples{$sorter}}, [$fname, $type];
        }
    }
    # Declare the return variable.
    my @retVal;
    for my $sorter (sort keys %tuples) {
        push @retVal, @{$tuples{$sorter}};
    }
    # Return the result.
    return @retVal;
}

=head3 validate_fields

    ERDBtk::Helpers::Scripts::validate_fields(\@fieldList, $objectData);

Throw an error if an invalid field name is present in the specified list.

=over 4

=item fieldList

Reference to a list of field names.

=item objectData

The descriptor for the relevant entity or relationship found in the L<ERDBtk> object.

=back

=cut

sub validate_fields {
    # Get the parameters.
    my ($fieldList, $objectData) = @_;
    # Get the field hash from the object descriptor.
    my $fieldH = $objectData->{Fields};
    # This will contain a list of invalid field names.
    my @badFields;
    for my $field (@$fieldList) {
        if (! exists $fieldH->{$field} || $fieldH->{$field}{imported}) {
            push @badFields, $field;
        }
    }
    # If we have invalid fields, throw an error.
    if (@badFields) {
        my $noun = (scalar(@badFields) > 1 ? 'names' : 'name');
        die "Invalid field $noun found: " . join(", ", @badFields);
    }
}


=head3 clean_results

    my $cleanedList = ERDBtk::Helpers::Scripts::clean_results(\@resultList);

This script looks at a single set of results from a query and removes duplicate lines.

=over 4

=item resultList

Reference to a list of sub-lists, each sub-list representing a line of output.

=item RETURN

Returns a reference to a list of sub-lists, each sub-list representing a unique line of
output.

=back

=cut

sub clean_results {
    my ($resultList) = @_;
    # This hash is used to remove duplicates.
    my %dupFilter;
    # This will be the return list.
    my @retVal;
    for my $result (@$resultList) {
        my $rKey = join("\t", @$result);
        if (! $dupFilter{$rKey}) {
            push @retVal, $result;
            $dupFilter{$rKey} = 1;
        }
    }
    # Return the result list.
    return \@retVal;
}



=head3 compute_field_list

    my @fieldList = ERDBtk::Helpers::Scripts::compute_field_list($all, $fields, $objectData, \@allFields);

Compute the list of return field names for a query script.

=over 4

=item all

TRUE if the C<all> option has been specified.

=item fields

The value of the C<fields> option (if specified).

=item objectData

The entity or relationship descriptor for the target object.

=item allFields

Reference to a list of field information tuples. The first element in each tuple should be the field name.
The fields should be all the returnable fields of the entity or relationship, in pretty-sort order.

=back

=cut

sub compute_field_list {
    my ($all, $fields, $objectData, $allFields) = @_;
    # This will contain the return list of fields.
    my @retVal;
    # Compute the field list.
    if ($all) {
        # Here we want all the fields.
        push @retVal, map { $_->[0] } @$allFields;
    } elsif ($fields) {
        # Here we have a comma-delimited field name list.
        @retVal = split(/,/, $fields);
        ERDBtk::Helpers::Scripts::validate_fields(\@retVal, $objectData);
    } else {
        # Here we do the default fields.
        my $defaults = $objectData->{default};
        if ($defaults) {
            # Most entities will have a default.
            push @retVal, split(/\s+/, $objectData->{default});
        } elsif (grep { $_->[0] eq 'id '} @$allFields) {
            # Here we have an entity with no default, so we just do the ID.
            # Otherwise, we default to no fields!!
            push @retVal, 'id';
        }
    }
    # Return the field list.
    return @retVal;
}


=head3 compute_filtering

    my ($filter, $parms) = ERDBtk::Helpers::Scripts::compute_filtering($is, $like, $op, $objectName, $objectData);

Compute the filtering data for a script query. The filter string and the
parameter list will be returned. The filter string is guaranteed to be a
simple conjunction (that is, zero or more simple clauses joined by
C<AND>).

=over 4

=item is

If specified, a reference to a list of strings, each containing a comma-delimited 2-tuple with (0) a
field name and (1) a value that must be in that field.

=item like

If specified, a reference to a list of strings, each containing a comma-delimited 2-tuple with (0) a
field name and (1) an SQL pattern that must match the value in that field.

=item op

If specified, a reference to a list of strings, each containing a comma-delimited 3-tuple with (0) a
field name, (1) an operator, and (2) a value that must stand in the proper relation to the value in
that field. The operator can be C<lt>, C<le>, C<gt>, C<ge>, C<eq>, or C<ne>.

=item objectName

The name of the target entity or relationship.

=item objectData

The descriptor from the L<ERDBtk> object for the target entity or relationship.

=item RETURN

Returns a two-element list consisting of (0) the filter string, and (1) the parameter list
for the desired query.

=back

=cut

use constant OPERATOR_MAP => { 'ne' => '<>', '!=' => '<>', '<>' => '<>',
                               'eq' => '=',  '='  => '=',  '==' => '=',
                               'lt' => '<',  '<'  => '<',
                               'le' => '<=', '<=' => '<=', '=<' => '<=',
                               'gt' => '>',  '>'  => '>',
                               'ge' => '>=', '>=' => '>=', '=>' => '>=',
};

sub compute_filtering {
    # Get the parameters.
    my ($is, $like, $op, $objectName, $objectData) = @_;
    # Get empty list for unspecified parameters.
    $is //= [];
    $like //= [];
    $op //= [];
    # Set up lists to accumulate parameters and filter clauses.
    my (@filter, @parms);
    # This list will accumulate field names for later validation.
    my @fieldNames;
    # Loop through the 2-tuple specifiers.
    for my $thing (['=',$is], ['LIKE',$like]) {
        my ($o, $list) = @$thing;
        for my $spec (@$list) {
            my ($field, $value) = split /,/, $spec;
            push @fieldNames, $field;
            push @filter, "$objectName($field) $o ?";
            push @parms, $value;
        }
    }
    # Loop through the op specifiers.
    for my $spec (@$op) {
        my ($field, $o, $value) = split /,/, $spec;
        push @fieldNames, $field;
        my $actualO = OPERATOR_MAP->{$o};
        if (! $actualO) {
            die "Invalid query operator '$o'.";
        } else {
            push @fieldNames, $field;
            push @filter, "$objectName($field) $actualO ?";
            push @parms, $value;
        }
    }
    # Validate the field names.
    validate_fields(\@fieldNames, $objectData);
    # Return the results.
    my $filter = '';
    if (@filter) {
        $filter = join(' AND ', @filter);
    }
    return ($filter, \@parms);
}


1;