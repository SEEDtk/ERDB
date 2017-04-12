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


package ProtFamRepo;

    use strict;
    use warnings;
    use P3DataAPI;

=head1 Accumulate Protein Family Information

This object accumulates information describing protein families. It has a hash mapping protein IDs to protein family
IDs and a second mapping protein family IDs to functions. The L</output> method can be used to spool the results
into files for loading into the Shrub.

The fields of this object are

=over 4

=item protFams

Reference to a hash keyed on protein ID that maps each protein to its parent family ID.

=item famFuns

Reference to a hash on on family ID that maps each family to its functional assignment description.

=item stats

A L<Stats> object for maintaining statistics.

=back

=head2 Special Methods

=head3 new

    my $protFamRepo = ProtFamRepo->new($stats);

Create a new, blank, protein family repository object.

=over 4

=item stats

A L<Stats> object to be used for maintaining statistics about the families.

=back

=cut

sub new {
    my ($class, $stats) = @_;
    my $retVal = { famFuns => {}, protFams => {}, stats => $stats };
    bless $retVal, $class;
    return $retVal;
}


=head2 Public Manipulation Methods

=head3 AddProt

    $protFamRepo->AddProt($famID, $protID, $function);

Add a protein to the family repository. We may or may not have a function from the client. If we do, it is
stored along with with protein. If we don't, and there is no known function yet, we put in a placeholder so
we can fill in the function later.

=over 4

=item protID

MD5 ID of the relevant protein.

=item famID

ID of the family containing the protein.

=item function (optional)

The function description for the family.

=back

=cut

sub AddProt {
    my ($self, $famID, $protID, $function) = @_;
    # Get the statistics object.
    my $stats = $self->{stats};
    # Get the protein hash.
    my $protFams = $self->{protFams};
    # Do we already have this protein?
    if (exists $protFams->{$protID}) {
        if ($protFams->{$protID} ne $famID) {
            $stats->Add(pfamProtFamMismatch => 1);
        } else {
            $stats->Add(pfamProtFamDuplicate => 1);
        }
    } else {
        # No, add the protein to the family.
        $protFams->{$protID} = $famID;
        $stats->Add(pfamProtFamStored => 1);
    }
    # Get the family hash.
    my $famFuns = $self->{famFuns};
    # Do we already have a function for this family?
    if ($famFuns->{$famID}) {
        $stats->Add(pFamFunDuplicate => 1);
    } else {
        # No. We must store it. Did the user provide a function?
        if ($function) {
            # Yes. Store the function.
            $famFuns->{$famID} = $function;
            $stats->Add(pFamFunStored => 1);
        } elsif (! exists $famFuns->{$famID}) {
            # No, but we must add a function entry as a placeholder.
            $famFuns->{$famID} = "";
            $stats->Add(pFamFunBlank => 1);
        }
    }
}

=head3 FixFunctions

    $protFamRepo->FixFunctions();

Query the PATRIC system to get the function information for families where the function is currently unknown.

=cut

sub FixFunctions {
    my ($self) = @_;
    my $stats = $self->{stats};
    # Get access to PATRIC.
    my $p3 = P3DataAPI->new();
    # Get the family-to-function hash.
    my $famFuns = $self->{famFuns};
    # Loop through the families, looking for those without functions.
    my @missing = grep { ! $famFuns->{$_} } sort keys %$famFuns;
    # Question PATRIC about these functions in batches of 200.
    while (scalar @missing) {
        # Pop 200 families off of the list.
        my @batch = splice @missing, 0, 200;
        my $count = scalar(@batch);
        # Ask PATRIC for their functions.
        print "Fetching $count protein family definitions from PATRIC.\n";
        $stats->Add(pFamFunRequested => $count);
        my $filter = '(' . join(',', @batch) . ')';
        my @rows = $p3->query(protein_family_ref => ['select', 'family_id', 'family_product'], ['in', 'family_id', $filter]);
        my $found = scalar(@rows);
        $stats->Add(pFamFunReturned => $found);
        print "$found functions returned.\n";
        if ($found < $count) {
            $stats->Add(pFamFunNotFound => ($count - $found));
        }
        # Loop through the results, storing the function IDs.
        for my $row (@rows) {
            my $fam = $row->{family_id};
            my $fun = $row->{family_product};
            $famFuns->{$fam} = $fun;
            $stats->Add(pFamFunStoredFromQuery => 1);
        }
    }
}

=head3 output

    $protFamRepo->output($dir);

Output the protein family data to files for loading. There will be a C<protFams.tbl> file that maps protein IDs to family
IDs and a C<famFuns.tbl> file that maps family IDs to functional assignments. Both files are tab-delimited, with the key
in the first column and the value in the second.

=over 4

=item dir

The name of the directory into which the output files should be placed.

=back

=cut

sub output {
    my ($self, $dir) = @_;
    # Open the protFams output file.
    open(my $oh, ">$dir/protFams.tbl") || die "Could not open protFams output file: $!";
    # Get the output hash.
    my $protFams = $self->{protFams};
    # Loop through it, producing output.
    for my $prot (sort keys %$protFams) {
        print $oh "$prot\t$protFams->{$prot}\n";
    }
    # Set up for the famFuns output file.
    close $oh; undef $oh;
    open($oh, ">$dir/famFuns.tbl") || die "Could not open famFuns output file: $!";
    # Get the output hash.
    my $famFuns = $self->{famFuns};
    # Loop through it, producing output.
    for my $fam (sort keys %$famFuns) {
        print $oh "$fam\t$famFuns->{$fam}\n";
    }
}

1;