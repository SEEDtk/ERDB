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
use File::Copy::Recursive;
use Shrub::ChemLoader;

=head1 Copy Data From one or more SEED FIGdisks to Create an Input Repository

    BuildRepo [ options ]

This script copies genomes and subsystems from one or more SEEDs into a format
ready for loading into Shrub.

=head2 Parameters

There are no positional parameters.

The command-line options are those found in L<CopyFromSeed/common_options>
plus L<ScriptUtils/ih_options>.

=head3 Standard Input

The file is driven by commands in the standard input, which is a tab-delimited
file. For each SEED being loaded, there must be a header line with the following fields.

=over 4

=item 1

The command word C<+SEED>.

=item 2

The path to the SEED's FIGdisk.

=item 3

The privilege level of the SEED-- C<0> (public), C<1> (projected), or C<2> (core). This
defaults to the privilege level specified in the C<--privilege> command-line option.

=back

Each subsequent line should contain one of the following.

=over 4

=item 1

A genome ID (recognized because it is two numbers separated by a period).

=item 2

The string C<*Genomes>, indicating all genomes should be loaded.

=item 3

The string C<*Subsystems>, indicating all subsystems should be loaded.

=item 4

A subsystem name. Anything that does not match one of the above is considered
to be a subsystem name.

=back

This allows a fairly flexible load from multiple sources.

=cut

# Start timing.
my $startTime = time;
$| = 1; # Prevent buffering on STDOUT.
# Get the command-line parameters.
my $opt = ScriptUtils::Opts('',
        CopyFromSeed::common_options(),
        ScriptUtils::ih_options(),
);
# Get a helper object and the associated statistics object.
my $loader = CopyFromSeed->new($opt);
my $stats = $loader->stats;
# Connect to the standard input.
my $ih = ScriptUtils::IH($opt->input);
# Are we clearing?
if ($opt->clear) {
    print "Erasing genome repository.\n";
    my $genomeDir = $loader->genome_repo();
    File::Copy::Recursive::pathempty($genomeDir) ||
        die "Error clearing $genomeDir: $!";
    print "Erasing subsystem repository.\n";
    my $subsysDir = $loader->subsys_repo();
    File::Copy::Recursive::pathempty($subsysDir) ||
        die "Error clearing $subsysDir: $!";
}
# Denote we don't have a SEED yet.
my $inSEED = 0;
# Loop through the input file.
while (! eof $ih) {
    my $line = <$ih>;
    chomp $line;
    $stats->Add(commandLines => 1);
    my ($command, @parms) = split /\t/, $line;
    # Determine the purpose of this line.
    if ($command eq '+SEED') {
        # Here we have a new SEED.
        my ($figDisk, $priv) = @parms;
        # Insure we have a FIG disk.
        if (! $figDisk) {
            die "No FIG disk specified on +SEED command line.\n";
        }
        # Default the privilege to 0.
        $priv //= $opt->privilege;
        # Tell the helper (and the user) about this SEED.
        my $mode = ($inSEED ? "Switching to" : "Copying from");
        print "$mode level-$priv SEED at $figDisk.\n";
        $loader->SetSEED($figDisk, $priv);
        # Denote we're in a SEED.
        $inSEED = 1;
    } elsif (! $inSEED) {
        # Other commands require a SEED be selected.
        die "You must select a SEED before issuing other commands.";
    } elsif ($command eq '*Subsystems') {
        # Here the user wants to load all subsystems.
        my $subList = $loader->ComputeSubsystems();
        # Loop through the subsystems, loading them.
        print "Processing subsystems.\n";
        my ($count, $total) = (0, scalar @$subList);
        for my $sub (@$subList) {
            $count++;
            print "Loading subsystem $count of $total.\n";
            $loader->CopySubsystem($sub);
        }
    } elsif ($command eq '*Genomes') {
        # Here the user wants to load all genomes.
        my $genomeList = $loader->AllGenomes();
        # Loop through the genomes, loading them.
        print "Processing genomes.\n";
        my($count, $total) = (0, scalar @$genomeList);
        for my $genome (@$genomeList) {
            $count++;
            print "Loading genome $count of $total.\n";
            $loader->CopyGenome($genome);
        }
    } elsif ($command =~ /^\d+\.\d+$/) {
        # Here the user wants to load a single genome.
        print "Loading single genome $command.\n";
        $loader->CopyGenome($command);
    } else {
        # Here we have a subsystem name.
        print "Loading single subsystem $command.\n";
        my $dirName = $loader->CheckSubsystem($command);
        if ($dirName) {
            $loader->CopySubsystem($dirName);
        }
    }
}
# If we have genome output, create the genome index.
$loader->IndexGenomes();
# Get the chemistry data.
print "Refreshing chemistry data.\n";
Shrub::ChemLoader::RefreshFiles();
# Compute the total time.
my $timer = time - $startTime;
$stats->Add(totalTime => $timer);
# Tell the user we're done.
print "All done.\n" . $stats->Show();

