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
    use File::Copy::Recursive;
    use Archive::Tar;

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

=item nameless

If TRUE, the genome names will be skipped, so that the return hash maps genome IDs directly to
directory names.

=back

=item RETURN

Returns a reference to a hash mapping genome IDs to 2-tuples consisting of (0) the directory name
and (1) the genome name. If the C<nameless> option is specified, returns a reference to a hash
mapping genome IDs directly to directory names.

=back

=cut

sub FindGenomeList {
    # Get the parameters.
    my ($self, $repository, %options) = @_;
    # The output will be put in here.
    my %retVal;
    # Remember the nameless option.
    my $nameless = $options{nameless};
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
                my $fullDir = "$repository/$dir";
                $retVal{$genome} = ($nameless ? $fullDir : [$fullDir, $name]);
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
                    if ($nameless) {
                        $retVal{$subDir} = $dirName;
                    } else {
                        # We need the genome name. Pull it from the metadata.
                        my $metaHash = $self->ReadMetaData("$dirName/genome-info", required => [qw(name)]);
                        $retVal{$subDir} = [$dirName, $metaHash->{name}];
                    }
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
    my $fixedName = DenormalizedName($subName);
    # Form the full directory name.
    my $retVal = "$subsysDirectory/$fixedName";
    # Verify that it exists.
    if (! -d $retVal) {
        die "Subsystem $subName not found in $subsysDirectory.";
    }
    # Return the directory name.
    return $retVal;
}


=head3 NormalizedName

    my $subName2 = $loader->NormalizedName($subName);

or

    my $subName2 = RepoLoader::NormalizedName($subName);

Return the normalized name of the subsystem with the specified name. A subsystem
name with underscores for spaces will return the same normalized name as a subsystem
name with the spaces still in it.

=over 4

=item subName

Name of the relevant subsystem.

=item RETURN

Returns a normalized subsystem name.

=back

=cut

sub NormalizedName {
    # Convert from the instance form of the call to a direct call.
    shift if UNIVERSAL::isa($_[0], __PACKAGE__);
    # Get the parameters.
    my ($subName) = @_;
    # Normalize the subsystem name by converting underscores to spaces.
    # Underscores at the beginning and end are not converted.
    my $retVal = $subName;
    my $trailer = chop $retVal;
    my $prefix = substr($retVal,0,1);
    $retVal = substr($retVal, 1);
    $retVal =~ tr/_;!/ :?/;
    $retVal = $prefix . $retVal . $trailer;
    # Return the result.
    return $retVal;
}


=head3 DenormalizedName

    my $dirName = $loader->DenormalizeName($subName);

or

    my $dirName = RepoLoader::DenormalizedName($subName);

Convert a subsystem name to the name of the corresponding directory. This involves translating characters that
are illegal in directory names to alternate forms.  Note that to make this process
reversible, underscores (C<_>), semicolons (C<;>), and exclamation points (C<!>) should be avoided in subsystem
names, though the underscore may be used at the beginning or end.

=over 4

=item subName

Relevant subsystem name.

=item RETURN

Returns a version of the subsystem name suitable for use as a directory name.

=back

=cut

sub DenormalizedName {
    # Convert from the instance form of the call to a direct call.
    shift if UNIVERSAL::isa($_[0], __PACKAGE__);
    # Get the parameters.
    my ($subName) = @_;
    # Translate the characters.
    my $retVal = $subName;
    $retVal =~ tr/ :?/_;!/;
    # Return the result.
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
        # Get this genome's directory and name.
        my ($genomeLoc, $name) = @{$genomeDirs->{$genome}};
        # Relocate the directory so that it is relative to the repository.
        my $genomeRelLoc = substr($genomeLoc, $repoNameLen + 1);
        # Write the ID, name, and directory to the output file.
        print $oh join("\t", $genome, $name, $genomeRelLoc) . "\n";
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
    $genus =~ s/[\[\]:\(\)\?]//g;
    $species =~ s/[\[\]:\(\)\?]//g;
    # Compute the desired path.
    my $retVal = "$genus/$species";
    # Return the result.
    return $retVal;
}


=head3 ExtractRepo

    $loader->ExtractRepo($sourceFile, $targetDir);

Extract a repository from a source archive file into a target directory. The target directory
will be erased first. Use of this method requires the L<Archive::Tar> module.

=over 4

=item sourceFile

C<tar.gz> file containing an archive of the input repository.

=item targetDir

Directory into which the new input repository should be placed.

=back

=cut

sub ExtractRepo {
    # Get the parameters.
    my ($self, $sourceFile, $targetDir) = @_;
    # Get the statistics object.
    my $stats = $self->stats;
    # Clear the target directory.
    print "Erasing $targetDir.\n";
    File::Copy::Recursive::pathempty($targetDir);
    # Create an iterator through the archive.
    my $next = Archive::Tar->iter($sourceFile, COMPRESS_GZIP);
    while (my $file = $next->()) {
        if (! $file->is_file) {
            $stats->Add(archiveNonFile => 1);
        } else {
            # Here we have a file in the archive. Check its name.
            $stats->Add(archiveFile => 1);
            if ($file->name =~ /((?:Genome|SubSystem)Data.+)/) {
                # Compute the new file name.
                my $newName = join("/", $targetDir, DenormalizedName($1));
                # Extract the file.
                my $ok = $file->extract($newName);
                $stats->Add(archiveFileExtracted => 1);
                if (! $ok) {
                    die "Error extracting into $newName.";
                }
            } else {
                $stats->Add(archiveFileSkipped => 1);
            }
        }
    }
}


1;
