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

The command-line options are those found in L<CopyFromSeed/subsys_options>,
L<CopyFromSeed/genome_options>, and L<CopyFromSeed/common_options>.


=cut

    # Start timing.
    my $startTime = time;
    $| = 1; # Prevent buffering on STDOUT.
    # Get the command-line parameters.
    my $opt = ScriptUtils::Opts('fig_disk', CopyFromSeed::subsys_options(),
            CopyFromSeed::genome_options(), CopyFromSeed::common_options(),
    );
    # Get a helper object and the associated statistics object.
    my $loader = CopyFromSeed->new($opt, $ARGV[0]);
    my $stats = $loader->stats;
    # Compute the list of subsystems to load.
    print "Determining list of subsystems to process.\n";
    my $subList = $loader->ComputeSubsystems();
    # Loop through the subsystems, loading them.
    print "Processing subsystems.\n";
    for my $sub (@$subList) {
        $loader->CopySubsystem($sub);
    }
    # Determine the remaining genomes.
    print "Determining list of genomes to process.\n";
    my $genomeList = $loader->ComputeGenomes();
    # Loop through the genomes, loading them.
    print "Processing genomes.\n";
    for my $genome (@$genomeList) {
        $loader->CopyGenome($genome);
    }
    # If we have genome output, create the genome index.
    $loader->IndexGenomes();
    # Compute the total time.
    my $timer = time - $startTime;
    $stats->Add(totalTime => $timer);
    # Tell the user we're done.
    print "All done.\n" . $stats->Show();

