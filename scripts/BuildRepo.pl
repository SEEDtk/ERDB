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
use CopyFromPatric;

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

You can also specify genomes to be loaded from PATRIC. To do this, start with a header line
containing the following fields.

=over 4

=item 1

The command word C<+PATRIC>.

=item 2

The privilege level to assign-- C<0> (public), C<1> (projected), or C<2> (core). This
defaults to the privilege level specified in the C<--privilege> command-line option.

=back

Each subsequent line should contain a SEED-style genome ID.

This allows a fairly flexible load from multiple sources.

In addition to the SEED and PATRIC specifications, there are the following special commands.

=over 4

=item +Samples

This specifies a Metagenome Samples directory. The directory name is given in the second column.
All subdirectories will be analyzed and converted into samples in Exchange Format.

=item +Taxonomy

The taxonomy files will be downloaded from the NCBI website and unpacked into the appropriate directory.

=back

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
# Get the chemistry data.
print "Refreshing chemistry data.\n";
Shrub::ChemLoader::RefreshFiles($opt->repo);
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
    my $sampleDir = $loader->sampleRepo;
    if (-d $sampleDir) {
        print "Erasing sample repository.\n";
        File::Copy::Recursive::pathempty($sampleDir) ||
            die "Error clearing $sampleDir: $!";
    }
}
# Get the first line.
my $line = <$ih>;
# Loop through the input file.
while (defined $line) {
    $line =~ s/[\r\n]+$//;
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
        print "Copying from level-$priv SEED at $figDisk.\n";
        $loader->SetSEED($figDisk, $priv);
        # Loop through the SEED commands.
        my $done;
        while (! eof $ih && ! $done) {
            $line = <$ih>;
            $stats->Add(subCommandLines => 1);
            if (! defined $line || substr($line, 0, 1) eq '+') {
                # Here we have a new section.
                $done = 1;
            } else {
                $line =~ s/[\r\n]+$//;
                if ($line eq '*Subsystems') {
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
                } elsif ($line eq '*Genomes') {
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
                } elsif ($line =~ /^(\d+\.\d+)/) {
                    # Here the user wants to load a single genome.
                    my $genome = $1;
                    print "Loading single genome $genome.\n";
                    $loader->CopyGenome($genome);
                } else {
                    # Here we have a subsystem name.
                    print "Loading single subsystem $line.\n";
                    my $dirName = $loader->CheckSubsystem($line);
                    if ($dirName) {
                        $loader->CopySubsystem($dirName);
                    }
                }
            }
        }
    } elsif ($command eq '+Taxonomy') {
        # Here we have a request to download taxonomy data.
        $loader->CopyTaxonomy($loader->taxRepo());
    } elsif ($command eq '+Samples') {
        # Here we have a sample directory.
        my ($sampleDir) = @parms;
        # Insure the directory is valid.
        if (! $sampleDir) {
            die "No directory specified for samples.";
        } elsif (! -d $sampleDir) {
            die "Invalid sample directory $sampleDir.";
        } else {
            # Upload the samples.
            print "Sample directory $sampleDir selected.\n";
            $loader->CopySamples($sampleDir, $loader->sampleRepo());
        }
    } elsif ($command eq '+PATRIC') {
        # Get the privilege.
        my ($priv) = @parms;
        $priv //= $opt->privilege;
        # Create the PATRIC helper.
        print "Copying from level-$priv PATRIC.\n";
        my $ploader = CopyFromPatric->new($opt);
        my $done;
        while (! eof $ih && ! $done) {
            $line = <$ih>;
            $stats->Add(subCommandLines => 1);
            if (! defined $line || substr($line, 0, 1) eq '+') {
                # Here we have a new section.
                $done = 1;
            } elsif ($line =~ /^(\d+\.\d+)/) {
                my $genome = $1;
                # Copy the genome.
                print "Copying PATRIC genome $genome.\n";
                $ploader->CopyGenome($genome);
            }
        }
        # Roll up the statistics.
        $stats->Accumulate($ploader->stats);
    }
    $line = <$ih>;
}
# Create the genome index.
$loader->IndexGenomes();
# Compute the total time.
my $timer = time - $startTime;
$stats->Add(totalTime => $timer);
# Tell the user we're done.
print "All done.\n" . $stats->Show();

