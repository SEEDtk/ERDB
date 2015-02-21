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


package RepoLoader;

    use strict;
    use warnings;
    use base qw(Loader);

=head1 Repository Loader Utilities

This package contains general utilities for accessing an input repository. It represents the
aspects of loading that do not touch the Shrub database.


=head2 Repository Management Methods

=head3 FindGenomeList

    my $genomeHash = $loader->FindGenomeList($repository, %options);

Find all the genomes in the specified repository directory. The result will list all the genome directories
and describe where to find the genomes. The genomes could be in a single flat directory or in a hierarchy that
we must drill down, so there is some recursion involved.

=over 4

=item repository

Directory name of the genome repository.

=item options

A hash of tuning options. The following keys are accepted.

=over 8

=item useDirectory

If TRUE, then any index file in the repository will be ignored and the directory hierarchy will be traversed.
Otherwise, the index file will be read if present.

=back

=item RETURN

Returns a reference to a hash mapping genome IDs to 2-tuples consisting of (0) the directory name
and (1) the genome name.

=back

=cut

sub FindGenomeList {
    # Get the parameters.
    my ($self, $repository, %options) = @_;
    # The output will be put in here.
    my %retVal;
    # Can we use an index file?
    my $indexUsed;
    if (! $options{useDirectory} && -f "$repository/index") {
        # Open the index file.
        if (! open(my $ih, "<$repository/index")) {
            print "Error opening $repository index file: $!\n";
        } else {
            # We have the index file. Read the genomes from it.
            print "Reading genome index for $repository.\n";
            while (my $fields = $self->GetLine(GenomeIndex => $ih)) {
                my ($genome, $name, $dir) = @$fields;
                $retVal{$genome} = ["$repository/$dir", $name];
            }
            # Denote we've loaded from the index.
            $indexUsed = 1;
        }
    }
    # Did we use the index file?
    if (! $indexUsed) {
        # No index file, we need to traverse the tree. This is a stack of directories still to process.
        my $genomeCount = 0;
        my @dirs = ($repository);
        while (@dirs) {
            # Get the next directory to search.
            my $dir = pop @dirs;
            # Retrieve all the subdirectories. This is a filtered search, so "." and ".." are skipped
            # automatically.
            my @subDirs = grep { -d "$dir/$_" } $self->OpenDir($dir, 1);
            # Loop through the subdirectories.
            for my $subDir (@subDirs) {
                # Compute the directory name.
                my $dirName = "$dir/$subDir";
                # Check to see if this is a genome.
                if ($subDir =~ /^\d+\.\d+$/) {
                    # Here we have a genome directory.
                    $retVal{$subDir} = $dirName;
                    $genomeCount++;
                    if ($genomeCount % 200 == 0) {
                        print "Reading genome directories. $genomeCount genomes processed.\n";
                    }
                } else {
                    # Here we have a subdirectory that might contain more genomes.
                    # Push it onto the stack to be processed later.
                    push @dirs, $dirName;
                }
            }
        }
        print "$genomeCount genomes found in $repository.\n";
    }
    # Return the genome hash.
    return \%retVal;
}

=head3 FindSubsystem

    my $subDir = RepoLoader::FindSubsystem($subsysDirectory, $subName);

or

    my $subDir = $loader->FindSubsystem($subsysDirectory, $subName);

Find the directory for the specified subsystem in the specified subsystem repository. Subsystem
directory names are formed by converting spaces in the subsystem name to underscores and using
the result as a directory name under the subsystem repository directory. This method will fail if
the subsystem directory is not found.

=over 4

=item subsysDirectory

Name of the subsystem repository directory.

=item subName

Name of the target subsystem.

=item RETURN

Returns the name of the directory containing the subsystem source files.

=back

=cut

sub FindSubsystem {
    # Convert the instance-style call to a direct call.
    shift if UNIVERSAL::isa($_[0],__PACKAGE__);
    # Get the parameters.
    my ($subsysDirectory, $subName) = @_;
    # Convert the subsystem name to a directory format.
    my $fixedName = $subName;
    $fixedName =~ tr/ /_/;
    # Form the full directory name.
    my $retVal = "$subsysDirectory/$fixedName";
    # Verify that it exists.
    if (! -d $retVal) {
        die "Subsystem $subName not found in $subsysDirectory.";
    }
    # Return the directory name.
    return $retVal;
}

=head3 IndexGenomes

    $loader->IndexGenomes($genomeDir);

Build the index for a genome repository. The index relates each genome ID
to its relative location in the repository's directory tree.

=over 4

=item genomeDir

Directory path to the genome repository.

=back

=cut
sub IndexGenomes {
    # Get the parameters.
    my ($self, $genomeDir) = @_;
    # Get the statistics object.
    my $stats = $self->stats;
    # Open the output file.
    open(my $oh, ">$genomeDir/index") || die "Could not open index file for output: $!";
    # Get the length of the repository directory name.
    my $repoNameLen = length($genomeDir);
    # Read the genome list. Note we suppress use of the index if it's already there.
    print "Reading genome directory $genomeDir.\n";
    my $genomeDirs = $self->FindGenomeList($genomeDir, useDirectory => 1);
    # Loop through the genomes.
    for my $genome (sort keys %$genomeDirs) {
        # Get this genome's directory.
        my $genomeLoc = $genomeDirs->{$genome};
        # Read its metadata.
        my $metaHash = $self->ReadMetaData("$genomeLoc/genome-info", required => 'name');
        # Relocate the directory so that it is relative to the repository.
        my $genomeRelLoc = substr($genomeLoc, $repoNameLen + 1);
        # Write the ID, name, and directory to the output file.
        print $oh join("\t", $genome, $metaHash->{name}, $genomeRelLoc) . "\n";
        $stats->Add(genomeOut => 1);
    }
    # Close the output file.
    close $oh;
    print "Genome directory index created.\n";
}

=head3 RepoPath

    my $relPath = $loader->RepoPath($genomeName);

Compute the repository path corresponding to a genome name. The
repository path is more or less determined by the genus and species.

=over 4

=item genomeName

Name of the genome whose repository path is to be computed.

=item RETURN

Returns a two-level directory path representing the relative location
in an input genome repository for this genome.

=back

=cut

sub RepoPath {
    # Get the parameters.
    my ($self, $genomeName) = @_;
    # Split on spaces to get the name components.
    my ($genus, $species, $strain) = split /\s+/, $genomeName;
    # If the species is "sp", use the strain.
    if (($species eq 'sp' || $species eq 'sp.') && $strain) {
        $species = $strain;
    } elsif (! $species) {
        $species = "sp";
    }
    # Remove dangerous characters.
    $genus =~ s/\[\]:\(\)//g;
    $species =~ s/\[\]:\(\)//g;
    # Compute the desired path.
    my $retVal = "$genus/$species";
    # Return the result.
    return $retVal;
}




1;