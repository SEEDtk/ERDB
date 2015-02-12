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
    use ShrubLoader;
    use Shrub;
    use MD5Computer;

=head1 Generate an Index and/or MD5s for Genome Source Directories

    ShrubIndexGenomes.pl [options] genomeDirectory

This script reads the hierarchy of a genome repository and creates its index file.

This script loads the DBD but does not connect to the database.

=head2 Parameters

The single positional parameter is the name of the genome directory.

The command-line options are as specified in L<Shrub/new_for_script> plus the
following.

=over 4

=item fixMD5

If specified, the script will recompute each genome's MD5 from its contigs file.

=back

=cut

    $| = 1; # Prevent buffering on STDOUT.
    # Connect to the database.
    my ($shrub, $opt) = Shrub->new_for_script('%c %o genomeDIrectory', { offline => 1 },
            ["fixMD5|f", "recompute MD5 identifiers in the genome-info files"]);
    # Create the loader object.
    my $loader = ShrubLoader->new($shrub);
    my $stats = $loader->stats;
    # Insure we have a genome directory.
    my ($genomeDir) = $ARGV[0];
    if (! $genomeDir) {
        $genomeDir = "$FIG_Config::shrub_dir/Inputs/GenomeData";
    }
    if (! -d $genomeDir) {
        die "Invalid genome directory $genomeDir.";
    }
    # Open the output file.
    open(my $oh, ">$genomeDir/index") || die "Could not open index file for output: $!";
    # Get the length of the repository directory name.
    my $repoNameLen = length($genomeDir);
    # Read the genome list. Note we suppress use of the index if it's already there.
    print "Reading genome directory $genomeDir.\n";
    my $genomeDirs = $loader->FindGenomeList($genomeDir, useDirectory => 1);
    # Loop through the genomes.
    for my $genome (sort keys %$genomeDirs) {
        # Get this genome's directory.
        my $genomeLoc = $genomeDirs->{$genome};
        # Read its metadata.
        my $metaHash = $loader->ReadMetaData("$genomeLoc/genome-info", required => 'name');
        # Relocate the directory so that it is relative to the repository.
        my $genomeRelLoc = substr($genomeLoc, $repoNameLen + 1);
        # Write the ID, name, and directory to the output file.
        print $oh join("\t", $genome, $metaHash->{name}, $genomeRelLoc) . "\n";
        $stats->Add(genomeOut => 1);
        # Are we fixing MD5s?
        if ($opt->fixmd5) {
            # Yes. Get the MD5 from the contigs file.
            my $correctMD5 = MD5Computer->new_from_fasta("$genomeLoc/contigs")->genomeMD5();
            $stats->Add(md5_checked => 1);
            if (! $metaHash->{md5} || $correctMD5 ne $metaHash->{md5}) {
                print "Correcting MD5 for $genome.\n";
                $stats->Add(md5_fixed => 1);
                $metaHash->{md5} = $correctMD5;
                $loader->WriteMetaData("$genomeLoc/genome-info", $metaHash);
            }
        }
    }
    # Close the output file.
    close $oh;
    # Tell the user we're done.
    print "Directory processed.\n" . $stats->Show();
