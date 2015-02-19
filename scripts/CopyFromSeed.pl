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
    use FIG_Config;
    use CopyFromSeed;
    use ScriptUtils;

=head1 Copy Data From a SEED FIGdisk

    CopyFromSeed [ options ] fig_disk

This script copies genomes and subsystems from the SEED into a format
ready for loading into Shrub.

=head2 Parameters

The single positional parameter is the location of the SEED FIGdisk
directory.

The command-line options are those found in L<CopyFromSeed/subsys_options> and
L<CopyFromSeed/genome_options> plus the following.

=cut

    # Start timing.
    my $startTime = time;
    $| = 1; # Prevent buffering on STDOUT.
    # Get the command-line parameters.
    my $opt = ScriptUtils::Opts('fig_disk', CopyFromSeed::subsys_options(),
            CopyFromSeed::genome_options());
    # Get a helper object and the associated statistics object.
    my $loader = CopyFromSeed->new($opt, $ARGV[0]);
    my $stats = $loader->stats;
    # Compute the list of subsystems to load.
    print "Determining list of subsystems to process.\n";
    my $subList = ComputeSubsystems($loader);
    # Loop through the subsystems, loading them.
    for my $sub (@$subList) {
        LoadSubsystem($sub, $loader);
    }
    # We no longer need the function hash. Release its memory.
    $loader->FreeFidFunctions();
    # Determine the remaining genomes.
    print "Determining list of genomes to process.\n";
    my $genomeList = FindRemainingGenomes($loader);
    for my $genome (@$genomeList) {
        LoadGenome($genome, $loader);
    }
    # Compute the total time.
    my $timer = time - $startTime;
    $stats->Add(totalTime => $timer);
    # Tell the user we're done.
    print "All done.\n" . $stats->Show();

=head2 Subroutines

=head3 ComputeSubsystems

    my $subList = ComputeSubsystems($loader);

Compute the list of subsystems to process. The subsystem names will be
converted to directory format and directories that are not found will be
eliminated.

=over 4

=item loader

L<CopyFromSeed> helper object.

=item RETURN

Returns a reference to a list of base subsystem directory names. These are
essentially the subsystem names with spaces converted to underscores.

=back

=cut

sub ComputeSubsystems {
    # Get the parameters.
    my ($loader) = @_;
    # Declare the return variable.
    my @retVal;

    ##TODO: Code for ComputeSubsystems
    # Return the result.
    return \@retVal;
}


=head3 LoadSubsystem

    LoadSubsystem($sub, $loader);

Extract the specified subsystem from the SEED and place its
exchange-format files in the desired subsystem output directory.

=over 4

=item sub

The directory name of the subsystem to process. (This is essentially
the subsystem name with spaces converted to underscores.)

=item loader

L<CopyFromSeed> helper object.

=back

=cut

sub LoadSubsystem {
    # Get the parameters.
    my ($sub, $loader) = @_;
    ##TODO: Code for LoadSubsystem
}

=head3 FindRemainingGenomes

    my $genomeList = FindRemainingGenomes($loader);

Return a list of the genomes still to be processed. Some genomes are
loaded while processing subsystems, and these are tracked in the
L<CopyFromSeed> object. This method gets the full list of genomes to
process and subtracts the ones already loaded.

=over 4

=item loader

L<CopyFromSeed> helper object.

=item RETURN

Returns a reference to a list of genome IDs for the genomes to process.

=back

=cut

sub FindRemainingGenomes {
    # Get the parameters.
    my ($loader) = @_;
    # Declare the return variable.
    my $retVal;
    ##TODO: Code for FindRemainingGenomes
    # Return the result.
    return $retVal;
}

=head3 LoadGenome

    LoadGenome($genome, $loader);

Extract a genome from the SEED directories and deposit its
exchange-format files in the designated genome output directory.

=over 4

=item genome

ID of the genome to process.

=item loader

L<CopyFromSeed> helper object.

=back

=cut

sub LoadGenome {
    # Get the parameters.
    my ($genome, $loader) = @_;
    ##TODO: Code for LoadGenome
}
