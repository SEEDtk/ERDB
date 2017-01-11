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


package Shrub::Contigs;

    use strict;
    use warnings;
    use base qw(Contigs);

=head1 Contig Management Object

This object contains the contigs for a specific genome. it provides methods for extracting
DNA and exporting the contigs in different forms.

The object is a subclass of L<Contigs> that gets the contig data from the L<Shrub> database.
In addition to the fields in the base-class object. It contains the following.

=over 4

=item shrub

L<Shrub> object for accessing the database.

=back


=head2 Special Methods

=head3 new

    my $contigObj = Shrub::Contigs->new($shrub, $genomeID);

Create a Contigs object for the specified genome ID. If the genome is not found, it will return an undefined value.

=over 4

=item shrub

L<Shrub> object for accessing the database.

=item genomeID

ID of the genome whose contigs are desired.

=back

=cut

sub new {
    # Get the parameters.
    my ($class, $shrub, $genomeID) = @_;
    # This will be the return variable.
    my $retVal;
    # Get the path to the genome's contig FASTA file.
    my $repo = $shrub->DNArepo;
    my ($contigPath, $geneticCode) = $shrub->GetEntityValues(Genome => $genomeID, 'contig-file genetic-code');
    # Only proceed if we found the genome.
    if ($contigPath) {
        my $contigFile = "$repo/$contigPath";
        # Create the contigs object.
        $retVal = Contigs::new($class, $contigFile, genomeID => $genomeID, genetic_code => $geneticCode);
        # Add the shrub reference.
        $retVal->{shrub} = $shrub;
    }
    # Return it.
    return $retVal;
}


=head2 Query Methods

=head3 fdna

    my $seq = $contigs->fdna($fid);

Return the DNA for the specified feature.

=over 4

=item fid

The ID of a feature in this object's genome.

=item RETURN

Returns a DNA sequence corresponding to the specified feature.

=back

=cut

sub fdna {
    # Get the parameters.
    my ($self, $fid) = @_;
    # Get the locations for the feature.
    my @flocs = $self->{shrub}->GetAll('Feature2Contig', 'Feature2Contig(from-link) = ? ORDER BY Feature2Contig(from-link), Feature2Contig(ordinal)',
            [$fid], 'to-link begin dir len');
    # Compute the DNA.
    my $retVal = $self->dna(@flocs);
    # Return the result.
    return $retVal;
}

1;