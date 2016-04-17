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


package Shrub::TaxonomyLoader;

    use strict;
    use warnings;
    use Data::Dumper;

=head1 Load Taxonomy Data

This package will load taxonomy data into the Shrub database. The NCBI taxonomy files must be present in
the C<Other> directory of the input repository.

This object contains the following fields, which are used to compute the taxonomic ID of a genome.

=over 4

=item idMap

Reference to a hash that maps obsolete taxonomic IDs to their new IDs.

=item nameMap

Reference to a hash that maps names to taxonomic IDs.

=item idNameMap

Reference to a hash that maps each taxonomic grouping ID to its primary scientific name.

=item loader

L<Shrub::DBLoader> object for accessing the database and the L<Stats> object.

=item parents

Reference to a hash that counts the number of children of a taxonomic group.

=back

=head2 Input Files

All of the taxonomy files use "\t|\t" as a field separator and "\t|\n" as a line separator.

The following files are used by this object to load the data.

=head3 nodes.dmp

The main input file, with one record per taxonomic grouping. It contains the following fields.

=over 4

=item tax_id

The taxonomy ID number.

=item parent_id

The ID of the parent grouping.

=item rank

The grouping rank (kingdom, genus, family, etc.)

=item embl_code

The locus-name prefix [not used].

=item division_id

The Genbank division ID.

=item inherited_division

C<1> if the division is inherited from the parent grouping.

=item genetic_code_id

The normal genetic code for organisms in this grouping.

=item inherited_genetic_code

C<1> if the genetic code is inherited from the parent grouping.

=item mitochondrial_genetic_code

The normal genetic code for mitochondrial chromosomes in this grouping.

=item inherited_mitochondrial_genetic_code

C<1> if the mitochondrial genetic code is inherited from the parent grouping

=item hidden

C<1> if this grouping should not appear in the taxonomy string

=item comments

free-form comments

=back

=head3 names.dmp

This contains the names for the groupings. It contains the following fields.

=over 4

=item tax_id

The ID of the taxonomic grouping associated with this name.

=item name_text

A name of the grouping.

=item unique_name

The unique variant of the name if it is not unique.

=item class

The type of name (common, synonym, scientific name).

=back

=head3 merged.dmp

=item old_id

The original taxonomy ID of a grouping.

=item new_id

The new ID of that grouping.

=back

=cut

    # This is a list of the tables we are loading.
    use constant LOAD_TABLES => qw(TaxonomicGrouping IsInTaxonomicGroup);


=head2 Special Methods

=head3 new

    my $taxLoader = Shrub::TaxonomyLoader->new($loader, %options);

Create a new, blank taxonomy loader object.

=over 4

=item loader

L<Shrub::DBLoader> object for accessing the database.

=item options

A hash of options, including zero or more of the following.

=item slow

TRUE if we are to load using individual inserts, FALSE if we are to load by spooling
inserts into files for mass loading. The default is FALSE.

=back

=cut

sub new {
    my ($class, $loader, %options) = @_;
    # Create the object.
    my $retVal = {
        loader => $loader,
        idMap => {},
        nameMap => {},
        idNameMap => {},
        parents => {}
    };
    # Initialize the tables. Note we have to erase them.
    if (! $options{slow}) {
        $loader->Open(LOAD_TABLES);
    }
    $loader->Clear(LOAD_TABLES);
    # Bless and return it.
    bless $retVal, $class;
    return $retVal;
}

=head2 Public Methods

=head3 LoadTaxonomies

    $taxLoader->LoadTaxonomies($repo);

Load the taxonomy data into the database and fill this object's hash tables.

=over 4

=item repo

Name of the input repository. The taxonomy files should be in the C<Other> subdirectory.

=back

=cut

# This is a list of useful name classes.
use constant NAME_CLASSES => { 'synonym' => 1, 'equivalent name' => 1, 'common name' => 1, 'misspelling' => 1 };

sub LoadTaxonomies {
    my ($self, $repo) = @_;
    # Get the two name-resolution hashes.
    my $nameMap = $self->{nameMap};
    my $idNameMap = $self->{idNameMap};
    # Get the id-mapping hash.
    my $idMap = $self->{idMap};
    # Get the child-counting hash.
    my $parents = $self->{parents};
    # Get the loader object.
    my $loader = $self->{loader};
    # Get the statistics object.
    my $stats = $loader->stats;
    # This hash will map each grouping ID to a list of names.
    my %idAliasH;
    # First we process the name file.
    print "Processing taxonomy names.\n";
    open(my $ih, "<$repo/Other/names.dmp") || die "Could not open names.dmp file: $!";
    while (! eof $ih) {
        # Get the next name.
        my ($taxID, $name, $unique, $type) = $self->read($ih);
        # Fix environmental samples.
        if ($name eq 'environmental samples' && $unique) {
                $name = $unique;
        }
        $stats->Add('taxnames-in' => 1);
        # This will be set to TRUE if we want to keep the name.
        my $keep;
        # Is this a scientific name?
        if ($type eq 'scientific name') {
            # Yes. Save it if it is the first for this ID.
            if (! exists $idNameMap->{$taxID}) {
                $idNameMap->{$taxID} = $name;
                    $stats->Add('taxnames-scientific' => 1);
            }
            $keep = 1;
        } elsif (NAME_CLASSES->{$type}) {
            # Here it's not scientific, but it's generally useful, so we keep it.
            $keep = 1;
            $stats->Add('taxnames-other' => 1);
        }
        # Associate this name with its ID.
        if ($keep) {
            $nameMap->{$name} = $taxID;
            push @{$idAliasH{$taxID}}, $name;
        }
    }
    close $ih; undef $ih;
    # Next we read the merges. These are used later to resolve taxonomic IDs of genomes.
    print "Processing taxonomy merges.\n";
    open($ih, "<$repo/Other/merged.dmp") || die "Could not open merged.dmp file: $!";
    while (! eof $ih) {
        my ($old_id, $new_id) = $self->read($ih);
        $stats->Add('taxon-merge' => 1);
        $idMap->{$old_id} = $new_id;
    }
    close $ih; undef $ih;
    # Finally, we read the nodes. Here is where we load the database.
    print "Reading taxonomic tree nodes.\n";
    open($ih, "<$repo/Other/nodes.dmp") || die "Could not open nodes.dmp file: $!";
    while (! eof $ih) {
        # Get the data for this group.
        my ($taxID, $parent, $type, undef, undef,
            undef,  undef,   undef, undef, undef, $hidden) = $self->read($ih);
        # Determine whether or not this is a domain group. A domain group is
        # terminal when doing taxonomy searches. The NCBI indicates a top-level
        # node by making it a child of the root node 1. We also include
        # super-kingdoms (archaea, eukaryota, bacteria), which are below cellular
        # organisms but are still terminal in our book.
        my $domain = ((($type eq 'superkingdom') || ($parent == 1)) ? 1 : 0);
        # Get the node's name.
        my $name = $idNameMap->{$taxID};
        # It's an error if there's no name.
        die "No name found for tax ID $taxID." if ! $name;
        # Count this as a child of its parent.
        $parents->{$parent}++;
        # Get the aliases.
        my $aliases = $idAliasH{$taxID} // [];
        # Create the taxonomy group record.
        $loader->InsertObject('TaxonomicGrouping', id => $taxID, domain => $domain, hidden => $hidden,
               'scientific-name' => $name, IsInTaxonomicGroup_link => $parent,
               alias => $aliases);
    }
}

=head3 ComputeTaxID

    my ($conf, $assignedTaxID) = $taxLoader->ComputeTaxID($genomeID, $taxID, $name);

Compute the taxonomic ID of a genome and our confidence in it. The preferred taxonomic ID is one found
explicitly in the genome exchange data. Failing that, we rely on the genome ID itself or the organism
name. This is a painful, complicated process.

=over 4

=item genomeID

ID of the genome in question.

=item taxID (optional)

Proposed explicit taxonomic ID.

=item name

Name of the organism.

=item RETURN

Returns a two-element list consisting of a confidence code (ranging from C<0> to C<5>) and the proposed
taxonomic ID.

=back

=cut

sub ComputeTaxID {
    my ($self, $genomeID, $taxID, $name) = @_;
    # Get the statistics object.
    my $stats = $self->{loader}->stats;
    # Get the id/name hashes. This one maps old IDs to new IDs.
    my $idMap = $self->{idMap};
    # This one maps IDs to scientific names.
    my $idNameMap = $self->{idNameMap};
    # This one maps names to IDs.
    my $nameMap = $self->{nameMap};
    # This contains the number of children of each taxonomic group.
    my $parents = $self->{parents};
    # We will store the confidence here.
    my $conf;
    # This will contain the new taxonomy ID.
    my $assignedTaxID;
    # Start by checking for an exact match in the alias table.
    my ($newTaxID) = $nameMap->{$name};
    if ($newTaxID) {
        # Get the information about this match.
        my $foundName = $idNameMap->{$newTaxID}; 
        # Determine our confidence in the match.
        if ($foundName eq $name) {
            $assignedTaxID = $newTaxID;
            $conf = 5;
        } elsif (defined $taxID && $taxID eq $newTaxID) {
            $assignedTaxID = $newTaxID;
            $conf = 4;
        } else {
            $assignedTaxID = $newTaxID;
            $conf = 2;
        }
    } else {
        if (defined $taxID) {
            # Here the name does not match, but we have a taxonomy ID. Verify that it is real.
            my ($taxName) = $idNameMap->{$taxID};
            if ($taxName) {
                # Here it's real. Verify that the assigned taxonomy ID is a leaf.
                if (! $parents->{$taxID}) {
                    # It is. Go for it.
                    $assignedTaxID = $taxID;
                    $conf = 3;
                }
            }
        }
        # Check to see if we have an assignment.
        if (! defined $conf) {
            # Try the genome ID.
            my ($newTaxID) = split /\./, $genomeID;
            # Check for a mapping.
            if ($idMap->{$newTaxID}) {
                $newTaxID = $idMap->{$newTaxID}; 
            }
            # Verify this ID is real.
            if ($idNameMap->{$newTaxID}) {
                # It is. Use it.
                $assignedTaxID = $newTaxID;
                $conf = 1;
            }
        }
        if (! defined $conf) {
            # Still no assignment. We have to guess.
            my @words = split /\s+/, $name;
            # Try looking for a substring match.
            while (! defined $conf && scalar(@words) >= 2) {
                my $guessName = join(" ", @words);
                my ($newTaxID) = $nameMap->{$guessName};
                if ($newTaxID) {
                    # Match found. Keep it.
                    $assignedTaxID = $newTaxID;
                    $conf = 0;
                } else {
                    # No match, so shorten the string.
                    pop @words;
                }
            }
        }
    }
    # Return the confidence and the assignment.
    return ($conf, $assignedTaxID);
}

=head2 Internal Utilities

=head3 read

    my @fields = $taxLoader->read($ih);

Read a taxonomy dump record and return its fields in a list. Taxonomy
dump records end in a tab-bar-newline sequence, and fields are separated
by a tab-bar-tab sequence, a more complex arrangement than is used in
standard tab-delimited files.

=over 4

=item ih

Open input handle for the taxonomy dump file.

=item RETURN

Returns a list of the fields in the record read.

=back

=cut

sub read {
    # Get the parameters.
    my ($self, $ih) = @_;
    # Get the stats object.
    my $stats = $self->{loader}->stats;
    # Temporarily change the end-of-record character.
    local $/ = "\t|\n";
    # Read the next record.
    my $line = <$ih>;
    $stats->Add(taxDumpRecords => 1);
    # Chop off the end, if any.
    if ($line =~ /(.+)\t\|\n$/) {
        $line = $1;
    }
    # Split the line into fields.
    my @retVal = split /\t\|\t/, $line;
    $stats->Add(taxDumpFields => scalar(@retVal));
    # Return the result.
    return @retVal;
}

1;