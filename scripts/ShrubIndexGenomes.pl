#!/usr/bin/env perl

#
# Copyright (c) 2003-2006 University of Chicago and Fellowship
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
use Stats;
use SeedUtils;
use RepoLoader;
use MD5Computer;
use ScriptUtils;

=head1 Generate an Index for Genome Source Directories

    ShrubIndexGenomes [options] genomeDirectory

This script reads the hierarchy of a genome repository and creates its index file.

=head2 Parameters

The single positional parameter is the name of the genome directory. There are no
command-line options.

=cut

$| = 1; # Prevent buffering on STDOUT.
# Process the command line.
my $opt = ScriptUtils::Opts('genomeDirectory');
# Get a Loader object so we have access to the metadata methods and indexing methods.
my $loader = RepoLoader->new();
my $stats = $loader->stats;
# Insure we have a genome directory.
my ($genomeDir) = $ARGV[0];
if (! $genomeDir) {
    $genomeDir = "$FIG_Config::shrub_dir/Inputs/GenomeData";
}
if (! -d $genomeDir) {
    die "Invalid genome directory $genomeDir.";
}
# Fix the genome index.
$loader->IndexGenomes($genomeDir);
# Tell the user we're done.
print "Directory processed.\n" . $stats->Show();
