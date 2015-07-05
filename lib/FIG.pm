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


package FIG;

    use strict;
    use warnings;
    use base qw(Shrub);

=head1 SEEDtk FIG Object Emulator

This object provides an extremely limited emulation of certain functions from the original FIG.pm module.
It is used for compatibility reasons when original SEED modules are required by SEEDtk. This object can
be constructed by calling either L<Shrub/new> or L<Shrub/new_for_script>, as in the following examples.
This object's methods are then available in addition to the standard L<Shrub> methods.

    my $fig = FIG->new_for_script($opt);

    my $fig = FIG->new(offline => 1, dbhost => 'localhost');


=head2 Public Methods

=head3 feature_location

    my $loc = $fig->feature_location($fid);

or

    my @loc = $fig->feature_location($fid);;

Return the location of a feature. The location consists
of a list of (contigID, begin, end) triples encoded
as strings with an underscore delimiter. So, for example,
C<NC_002755_100_199> indicates a location starting at position
100 and extending through 199 on the contig C<NC_002755>. If
the location goes backward, the start location will be higher
than the end location (e.g. C<NC_002755_199_100>).

In a scalar context, this method returns the locations as a
comma-delimited string

    NC_002755_100+99,NC_002755_210+288

In a list context, the locations are returned as a list

    (NC_002755_100+99, NC_002755_210+288)

=over 4

=item fid

ID of the feature whose location is desired.

=item RETURN

Returns the locations of a feature, either as a comma-delimited
string or a list.

=back

=cut

sub feature_location {
    # Get the parameters.
    my($self,$feature_id) = @_;
    # Get the feature's location list.
    my @locs = $self->fid_locs($feature_id);
    # Convert them to strings.
    my @retVal = map { $_->String() } @locs;
    # Return them in the fashion desired by the caller.
    if (wantarray()) {
        return @retVal;
    } else {
        return join(",", @retVal);
    }
}

=head3 get_translation

    my $translation = $fig->get_translation($prot_id);

The system takes any number of sources of protein sequences as input (and builds an nr
for the purpose of computing similarities).  For each of these input fasta files, it saves
(in the DB) a filename, seek address and length so that it can go get the translation if
needed.  This routine returns the stored protein sequence of the specified PEG feature.

=over 4

=item prot_id

ID of the feature (PEG) whose translation is desired.

=item RETURN

Returns the protein sequence string for the specified feature.

=back

=cut

sub get_translation {
    # Get the parameters.
    my($self, $prot_id) = @_;
    # Get the protein translation.
    my ($retVal) = $self->GetFlat('Feature Protein', 'Feature(id) = ?', [$prot_id], 'Protein(sequence)');
    # Return it.
    return $retVal;
}

=head3 md5_of_peg

    my $cksum = $fig->md5_of_peg( $peg );

Return the MD5 checksum for a peg. The MD5 checksum is computed from the
uppercase sequence of the protein.  This method retrieves the checksum stored
in the database.

=over 4

=item peg

FIG ID of the peg.

=item RETURN

Returns the checksum of the specified contig as a hex string, or C<undef> if
the peg is not in the database.

=back

=cut

sub md5_of_peg {
    # Get the parameters.
    my( $self, $peg ) = @_;
    # Get the feature's protein ID.
    my ($retVal) = $self->GetFlat('Feature2Protein', 'Feature2Protein(from-link) = ?', [$peg], 'to-link');
    # Return it.
    return $retVal;
}


1;