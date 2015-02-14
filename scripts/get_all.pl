#!/usr/bin/env perl
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


    use strict;
    use warnings;
    use Shrub;
    use ScriptUtils;

=head1 Perform a General Database Query

    get_all [ options ] -p path -c ' constraint' -v parm1 -v parm2 ... field1 field2 ...

This script performs a general database query. It can either perform a standalone query 
or process data from a tab-delimited input file. The L<ERDB/GetAll> command is used to 
perform the processing, and the documentation there should be consulted for more
details.

Essentially, to navigate the database you must specify a I<path> through the entities and
relationships, a I<constraint> that limits the output, one or more I<parameters> to the
constraint, and a list of I<fields> to be returned.

=head2 Parameters

The positional parameters are the names of the fields to be returned in order.
The field names should be specified in L<ERDB/Standard Field Name Format>.

The command-line options are those found in L<Shrub/new_for_script> plus
the following.

=over 4

=item path

A list of entity and relationship names, space-delimited, describing a
path through the database. If there is more than one name in the path,
you will have to enclose it in quotes. 

=item constraint

A query constraint, in the form of an SQL WHERE clause (without the word
C<WHERE>). The database field names must be specified in L<ERDB/Standard Field Name Format>,
and you can specify parameterizations using question marks. The question marks
will be replaced by the values of the I<value> option, in order.

=item value

A list of the values to be substituted for the parameter marks. This is an option
array, so if you need to specify more than one value, you simply specify the
option more than once. If you are using an input file, you can specify a value
taken from a column of the current input record using a dollar sign (C<$>)
followed by a column number. The column numbers are 1-based so a value parameter of
C<$1> would indicate the first column of the input file.

=item input

The name of the input file. If omitted, the standard input is used. The input
file is only used if one or more occurrences of the I<value> parameter
indicates an input column.

=item count

Maximum number of records to return. The default is C<0>, meaning all records
will be returned. If an input file is being used, this constraint applies to
each row of input. So, a count of 5 with an input file of 10 records would mean
a maximum of 50 results.  

## more command-line options

=back

=cut

    # Connect to the database and get the command parameters.
    my ($shrub, $opt) = Shrub->new_for_script('%c %o parm1 parm2 ...', { },
    		["path|p=s", "path through the database", { required => 1} ],
    		["constraint|c=s", "query constraint (if any)"],
    		["value|v=s@", "parameter values for the query constraint (multiple)"],
            ["input|i=s", "name of the input file (if not the standard input)"],
          	["count|n=i", "maximum number of records to output per input line (default is 0, meaning return everything)", { default => 0 }]
            );
    # Get the list of parameter values. If none were supplied, we use an empty list.
    my $valueList = $opt->value // [];
    # Check for input markers. This list will contain undef if the parameter value is a constant,
    # and a column index if the parameter value is taken from the input line. 
    my @inputList;
    # This will be set to the input file handle if we find at least one input marker.
    my $ih;
    # Loop through the parameter values.
    my $n = scalar @$valueList;
    for (my $i = 0; $i < $n; $i++) {
    	if ($valueList->[$i] =~ /^\$(\d+)/) {
    		# We have an input column marker. Save the column number.
    		push @inputList, ($1 - 1);
    		# If this is our first marker, open the input file.
    		if (! defined $ih) {
		    	$ih = ScriptUtils::IH($opt->input);
    		}
    	} else {
    		# Here we have an ordinary constant value.
    		push @inputList, undef;
    	}
    }
    # Now get the path, constraint, and count. The count already defaults to 0 so we don't
    # need to worry about it being undefined.
    my $path = $opt->path;
    my $constraint = $opt->constraint // '';
    my $count = $opt->count;
    # Get the list of output field names.
    my @fields = @ARGV;
    if (! scalar(@fields)) {
    	die "No output fields specified.\n";
    }
    # Get the first line of input. If we have no input, this is an empty string.
    my $line = (defined $ih ? <$ih> : "\n");
    # Loop until we run out of input.
    while (defined $line) {
    	# Parse the input line.
    	chomp $line;
    	my @cols = split /\t/, $line;
    	# Form the list of parameter values.
    	my @parms;
    	for (my $i = 0; $i < $n; $i++) {
    		if (defined $inputList[$i]) {
    			push @parms, $cols[$inputList[$i]];
    		} else {
    			push @parms, $valueList->[$i];
    		}
    	}
    	# Query the database.
    	my @rows = $shrub->GetAll($path, $constraint, \@parms, \@fields, $count);
    	# Output the results.
    	for my $row (@rows) {
    		print join("\t", @cols, @$row) . "\n";
    	}
    	# Get the next input line (or undef if there is no input file).
    	$line = (defined $ih ? <$ih> : undef);
    }
	# All done. Close the input.
	close $ih;
