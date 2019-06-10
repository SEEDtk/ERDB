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
use ProtFamRepo;

=head1 Copy Data From One or More Sources to Create an Input Repository

    BuildRepo [ options ]

This script copies genomes and subsystems from one or more sources into a format
ready for loading into Shrub.

=head2 Parameters

There are no positional parameters.

The command-line options are those found in L<CopyFromSeed/common_options>
plus L<ScriptUtils/ih_options>, plus the following.

=over 4

=item resume

Resume copying after a failed previous load.

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

=item 3

(optional) The name of a tab-delimtied file containing PATRIC genome IDs in its first column.

=back

If the file name is omitted, each subsequent line should contain a PATRIC genome ID.

Similarly, you can load from a RAST instance. To do this, start with a header line
containing the following fields.

=over 4

=item 1

The command word C<+RAST>.

=item 2

The root job directory of the RAST instance (e.g. C</vol/rast-prod/jobs>).

=item 3

The privilege level to assign-- C<0> (public), C<1> (projected), or C<2> (core). This
defaults to the privilege level specified in the C<--privilege> command-line option.

=back

Each subsequent line should have two fields.

=over 4

=item 1

The genome ID (SEED-style).

=item 2

The RAST job number.

=back

In addition to the SEED, RAST, and PATRIC specifications, there are the following special commands.

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
        ['resume', 'resume after an interrupted load']
);
# Get a helper object and the associated statistics object.
my $loader = CopyFromSeed->new($opt);
my $stats = $loader->stats;
# Create the protein family repo.
my $protFamRepo = ProtFamRepo->new($stats);
# If we are NOT resuming, get the chem data.
if (! $opt->resume) {
    # Get the chemistry data.
    print "Refreshing chemistry data.\n";
    Shrub::ChemLoader::RefreshFiles($opt->repo);
}
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
# This hash is used to prevent us from reloading genomes we've already processed.
my %genomesProcessed;
if ($opt->resume) {
    # We are resuming.  Denote that we've processed any genome that has a nonempty
    # "non-peg-info" file. To do this, we need to do a recursive search through
    # the GenomeData directory.
    my $genomeDir = $loader->genome_repo();
    my $genomeHash = $loader->FindGenomeList($genomeDir, nameless => 1);
    # Loop through the genomes in the directory, checking for "non-peg-info".
    print "Scanning for existing genomes.\n";
    my $count = 0;
    for my $genome (keys %$genomeHash) {
        my $dirName = $genomeHash->{$genome};
        if (-s "$dirName/non-peg-info") {
            $genomesProcessed{$genome} = 1;
            $stats->Add("genome-alreadyLoaded" => 1);
            $count++;
        }
    }
    print "$count pre-existing genomes found.\n";
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
        $loader->SetSEED($figDisk, $priv, \%genomesProcessed, $protFamRepo);
        # Loop through the SEED commands.
        my $done;
        while (! $done) {
            $line = <$ih>;
            $stats->Add(subCommandLines => 1);
            if (! defined $line || substr($line, 0, 1) eq '+') {
                # Here we have a new section.
                $done = 1;
            } else {
                $line =~ s/[\r\n]+$//;
                if ($line eq '*Subsystems') {
                    # Here the user wants to load all subsystems.  Get a list of the good ones.
                    my $subList = $loader->ComputeSubsystems();
                    # If we are resuming, delete any existing subsystems.
                    if ($opt->resume) {
                        print "Deleting old subsystem data.\n";
                        File::Copy::Recursive::pathempty($loader->subsys_repo);
                    }
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
        $line = <$ih>;
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
        $line = <$ih>;
    } elsif ($command eq '+PATRIC') {
        # Get the privilege and the optional input file.
        my ($priv, $file) = @parms;
        if (! defined $priv || $priv eq '') {
            $priv = $opt->privilege;
        }
        # Create the PATRIC helper.
        print "Copying from level-$priv PATRIC.\n";
        my $ploader = CopyFromPatric->new($priv, $opt, \%genomesProcessed, $protFamRepo);
        # Determine how we're finding the genome IDs.  The default is to use the current file.
        my $fh;
        if ($file) {
            print "Reading genome IDs from $file.\n";
            open($fh, '<', $file) || die "Could not open genome ID input file: $!";
        } else {
            print "Reading genome IDs from input file.\n";
            $fh = $ih;
        }
        my $done;
        while (! $done) {
            $line = <$fh>;
            $stats->Add(patricGenomeLines => 1);
            if (! defined $line || substr($line, 0, 1) eq '+') {
                # Here we have a new section.
                $done = 1;
                # If we read from a file, push forward to the next line.
                if ($file) {
                    $line = <$ih>;
                }
            } elsif ($line =~ /^(\d+\.\d+)/) {
                my $genome = $1;
                # Copy the genome.
                $ploader->CopyGenome($genome);
            } else {
                print "Invalid PATRIC input line: $line";
                $stats->Add(badInput => 1);
            }
        }
        # Roll up the statistics.
        $stats->Accumulate($ploader->stats);
    } elsif ($command eq '+RAST') {
        # Get the RAST directory and privilege level.
        my ($rastDir, $priv) = @parms;
        $priv //= $opt->privilege;
        # Reset the loader in RAST mode.
        $loader->Reset($priv, \%genomesProcessed, $protFamRepo, 1);
        print "Copying from level-$priv RAST at $rastDir.\n";
        $stats->Add(rastInstances => 1);
        # Loop through the genomes.
        my $done;
        while (! $done) {
            $line = <$ih>;
            $stats->Add(subCommandLines => 1);
            if (! defined $line || substr($line, 0, 1) eq '+') {
                # Here we have a new section.
                $done = 1;
            } elsif ($line =~ /^(\d+\.\d+)\t(\d+)/) {
                my ($genome, $job) = ($1, $2);
                # Copy the genome.
                print "Copying RAST genome $genome.\n";
                $loader->CopyGenome($genome, "$rastDir/$job/rp/$genome");
            } else {
                print "Invalid RAST input line: $line";
                $stats->Add(badInput => 1);
            }
        }
    } else {
        print "ERROR: Unknown command $command.\n";
        $stats->Add(badCommands => 1);
        $line = <$ih>;
    }
}
print "End of command file.\n";
# Create the genome index.
$loader->IndexGenomes();
# Unspool the family data.
print "Fixing Protein Families.\n";
$protFamRepo->FixFunctions();
print "Unspooling Protein Families.\n";
$protFamRepo->output($opt->repo . '/Other');
# Compute the total time.
my $timer = time - $startTime;
$stats->Add(totalTime => $timer);
# Tell the user we're done.
print "All done.\n" . $stats->Show();

