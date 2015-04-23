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

    my $gto = Shrub::Genome->new($shrub, $genomeID);

Create a new L<GenomeTypeObject> from a genome in the Shrub database.

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
    # Create the GTO.
    my $retVal = GenomeTypeObject::new($class);
    # Get the genome data.
    my ($name, $domain, $gc) = $shrub->GetEntityValues(Genome => $genomeID, 'name domain genetic-code');
    # Only proceed if we found the genome.
    if ($name) {
        $retVal->set_metadata({ id => $genomeID, scientific_name => $name, domain => $domain,
                genetic_code => $gc, source => 'Shrub', source_id => $genomeID });
        # Get the contigs.
        my $contigs = Shrub::Contigs->new($shrub, $genomeID);
        my @contigHashes = map { { id => $_->[0], dna => $_->[2] } } $contigs->tuples;
        $retVal->add_contigs(\@contigHashes);
        # Get the features.
        my $fpattern = "fig|$genomeID.%";
        my %fids = map { $_->[0] => { -id => $_->[0], -type => $_->[1], -function => $_->[2], -location => [] } }
                $shrub->GetAll('Genome2Feature Feature Feature2Function Function',
                'Feature2Function(from-link) LIKE ? ORDER BY Feature2Function(from-link), Feature2Function(security) DESC',
                [$fpattern], 'Feature(id) Feature(feature-type) Function(description)');
        # Compute the location strings.
        my $q = $shrub->Get('Feature2Contig', 'Feature2Contig(from-link) LIKE ?', [$fpattern], 'from-link to-link begin dir len');
        while (my $rec = $q->Fetch()) {
            my ($fid, $contig, $beg, $dir, $len) = $rec->Values('from-link to-link begin dir len');
            push @{$fids{$fid}{-location}}, [$contig, $beg, $dir, $len];
        }
        # Compute the translations.
        $q = $shrub->Get('Feature2Protein Protein', 'Feature2Protein(from-link) LIKE ?', [$fpattern],
                'Feature2Protein(from-link) Protein(sequence)');
        while (my $rec = $q->Fetch()) {
            my ($fid, $seq) = $rec->Values('Feature2Protein(from-link) Protein(sequence)');
            $fids{$fid}{-protein_translation} = $seq;
        }
        # Add the features.
        for my $fid (keys %fids) {
            $retVal->add_feature($fids{$fid});
        }
    }
    # Return the object.
    return $retVal;
}


1;