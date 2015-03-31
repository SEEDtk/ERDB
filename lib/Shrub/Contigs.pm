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

=head2 Special Methods

=head3 new

    my $contigObj = Shrub::Contigs->new($shrub, $genomeID);

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
    # Get the path to the genome's contig FASTA file.
    my $repo = $shrub->DNArepo;
    my ($contigPath, $geneticCode) = $shrub->GetEntityValues(Genome => $genomeID, 'contig-file genetic-code');
    my $contigFile = "$repo/$contigPath";
    # Create the contigs object.
    my $retVal = Contigs::new($class, $contigFile, genomeID => $genomeID, genetic_code => $geneticCode);
    # Return it.
    return $retVal;
}


1;