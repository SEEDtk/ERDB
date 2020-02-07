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


package Shrub::GTO;

    use strict;
    use warnings;
    use base qw(GenomeTypeObject);
    use Shrub::Contigs;

=head1 Create a Genome Type Object from a Shrub Genome

This is a subclass of a L<GenomeTypeObject> created from a genome in the Shrub database.

=head2 Special Methods

=head3 new

    my $gto = Shrub::GTO->new($shrub, $genomeID);

Create a new L<GenomeTypeObject> from a genome in the Shrub database. If the genome is not found, it will return
an undefined value.

=over 4

=item shrub

L<Shrub> object used to access the database.

=item genomeID

ID of the genome to load into the object.

=back

=cut

sub new {
    # Get the parameters.
    my ($class, $shrub, $genomeID) = @_;
    # Declare the return variable.
    my $retVal;
    # Get the genome data.
    my ($name, $domain, $gc, $core) = $shrub->GetEntityValues(Genome => $genomeID, 'name domain genetic-code core');
    # Only proceed if we found the genome.
    if ($name) {
        # Compute the taxonomy ID.
        my ($taxonID) = $shrub->GetFlat('Genome2Taxonomy', 'Genome2Taxonomy(from-link) = ?', [$genomeID], 'to-link');
        # Create a blank GTO.
        $retVal = GenomeTypeObject::new($class);
        # Fill in the base data.
        $retVal->set_metadata({ id => $genomeID, scientific_name => $name, domain => $domain,
                genetic_code => $gc, source => 'Shrub', source_id => $genomeID,
                ncbi_taxonomy_id => $taxonID });
        # Store the home.
        $retVal->{home} = ($core ? "CORE" : "PATRIC");
        # Get the contigs.
        my $contigs = Shrub::Contigs->new($shrub, $genomeID);
        my @contigHashes = map { { id => $_->[0], dna => $_->[2] } } $contigs->tuples;
        $retVal->add_contigs(\@contigHashes);
        # Get the features.
        my $fpattern = "fig|$genomeID.%";
        my %fids = map { $_->[0] => { -id => $_->[0], -type => $_->[1], -function => $_->[2], -location => [] } }
                $shrub->GetAll('Feature Feature2Function Function',
                'Feature2Function(from-link) LIKE ? ORDER BY Feature2Function(from-link), Feature2Function(security) DESC',
                [$fpattern], 'Feature(id) Feature(feature-type) Function(description)');
        # Compute the location strings.
        my $q = $shrub->Get('Feature2Contig', 'Feature2Contig(from-link) LIKE ?', [$fpattern], 'from-link to-link begin dir len');
        while (my $rec = $q->Fetch()) {
            my ($fid, $contig, $beg, $dir, $len) = $rec->Values('from-link to-link begin dir len');
            if ($dir eq '-') {
                $beg = $beg + $len - 1;
            }
            push @{$fids{$fid}{-location}}, [$contig, $beg, $dir, $len];
        }
        # Compute the translations.
        $q = $shrub->Get('Feature2Protein Protein', 'Feature2Protein(from-link) LIKE ?', [$fpattern],
                'Feature2Protein(from-link) Protein(sequence)');
        while (my $rec = $q->Fetch()) {
            my ($fid, $seq) = $rec->Values('Feature2Protein(from-link) Protein(sequence)');
            $fids{$fid}{-protein_translation} = $seq;
        }
        # Compute the protein families.
        $q = $shrub->Get('Feature2Protein Protein2Family ProteinFamily Family2Function Function', 'Feature2Protein(from-link) LIKE ?',
                [$fpattern], 'Feature2Protein(from-link) ProteinFamily(id) Function(description)');
        while (my $rec = $q->Fetch()) {
            my ($fid, $family, $func) = $rec->Values('Feature2Protein(from-link) ProteinFamily(id) Function(description)');
            if ($family =~ /^GF(.+)/) {
                $family = "PGF_$1";
                push @{$fids{$fid}{-family_assignments}}, ['PGFAM', $family, $func];
            }
        }
        # Compute the taxonomy.
        my $taxID = $retVal->{ncbi_taxonomy_id};
        my @lineage;
        while ($taxID != 1) {
            my ($taxData) = $shrub->GetAll('TaxonomicGrouping IsInTaxonomicGroup', 'TaxonomicGrouping(id) = ?', [$taxID], 'scientific-name id type IsInTaxonomicGroup(to-link)');
            my $next = pop @$taxData;
            unshift @lineage, $taxData;
            $taxID = $next;
        }
        $retVal->{ncbi_lineage} = \@lineage;
        # Add the features.
        for my $fid (keys %fids) {
            $retVal->add_feature($fids{$fid});
        }
    }
    # Return the object.
    return $retVal;
}


1;
