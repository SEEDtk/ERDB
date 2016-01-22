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
use ScriptUtils;
use File::Copy::Recursive;
use Stats;

=head1 Copy Protein Family Data

    CopyPFams.pl [ options ] sourceDirectory

This script splits the protein family file into smaller pieces that can be safely tarred up for
the L<ShrubLoad.pl> script. The protein family file is taken from the specified source directory
and unspooled into the C<Other/ProteinFamily> directory of the SEEDtk Input repository.

The output files should contain whole families. The basic strategy is to write an output file
until its size passes a certain value, then start a new one.

=head2 Parameters

The only positional parameter is the name of the source directory containing the file
C<merged.1.1.nr.only.with.md5>. The command-line options are as follows.

=over 4

=item maxlen

Cutoff length for an output file, in megabytes. As soon as an output file gets greater than this length, it
is closed and a new one started. The default is C<512>.

=back

=cut

# Get the command-line parameters.
my $opt = ScriptUtils::Opts('sourceDirectory',
        ['maxlen|m=i', 'cutoff length for output files in kilobytes', { default => 512 }],
        );
# Get the statistics object.
my $stats = Stats->new();
# Clear the existing output files.
my $outDir = "$FIG_Config::data/Inputs/Other/ProteinFamily";
if (! -d $outDir) {
    print "Creating $outDir.\n";
    File::Copy::Recursive::pathmk($outDir);
} else {
    print "Clearing $outDir.\n";
    File::Copy::Recursive::pathempty($outDir);
}
# Open the input file.
my ($sourceDir) = @ARGV;
my $inFile;
if (! -d $sourceDir) {
    die "Invalid source directory $sourceDir.";
} else {
    $inFile = "$sourceDir/merged.1.1.nr.only.with.md5";
    if (! -f $inFile) {
        die "Input file $inFile not found.";
    }
}
open(my $ih, "<$inFile") || die "Could not open $inFile: $!";
# Get the cutoff length.
my $cutoff = $opt->maxlen * (1024 * 1024);
# We want to start with a new file, so prime us as being after a cutoff.
my $currentFamily = "";
my $currentLength = $cutoff;
# Position on the first output file.
my $oh;
my $fileIndex = 0;
# Loop through the input.
while (! eof $ih) {
    # Get the next line.
    my $line = <$ih>;
    chomp $line;
    my ($family, undef, undef, undef, undef, $func, undef, undef, undef, $md5) = split /\t/, $line;
    $stats->Add(lineIn => 1);
    if ($family ne $currentFamily) {
        # Here we are starting a new protein family.
        $stats->Add(familyFound => 1);
        if ($currentLength >= $cutoff) {
            # Here we need a new file. Insure the old one is closed.
            if ($oh) {
                print "File $fileIndex closed at $currentLength bytes.\n";
                close $oh; undef $oh;
            }
            # Start the new file.
            $fileIndex++;
            open($oh, ">$outDir/protFamily.$fileIndex.tbl") || die "Could not open output file $fileIndex: $!";
            $currentLength = 0;
            print "File $fileIndex started.\n";
            $stats->Add(outFiles => 1);
        }
        # Start the new family.
        $currentFamily = $family;
    }
    # Write this line.
    my $newLine = "$family\t$func\t$md5\n";
    print $oh $newLine;
    $stats->Add(outLines => 1);
    $currentLength += length($newLine);
}
# Close the last file.
print "Final file $fileIndex closed at $currentLength bytes.\n";
close $oh;
print "All done.\n" . $stats->Show();