#!/usr/bin/perl -w

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

package ShrubGenomeLoader;

    use strict;
    use MD5Computer;

=head1 Shrub Genome Load Utilities

This package contains utilities for loading genomes. In particular, it contains methods for curating a
genome list to prevent duplicates and processing the genome contigs.

The object has the following fields.

=over 4

=item loader

L<ShrubLoader> object for accessing the database and statistics.

=item md5

L<MD5Computer> object for computing genome and contig MD5s.

=back

=head2 Special Methods

=head3 new

    my $genomeLoader = ShrubGenomeLoader->new($loader);

Construct a new Shrub function loader object and initialize the hash tables.

=over 4

=item loader

L<ShrubLoader> object to be used to access the database and the load utility methods.

=back

=cut

sub new {
    # Get the parameters.
    my ($class, $loader) = @_;
    # Create the object.
    my $retVal = { loader => $loader, md5 => undef };
    # Bless and return the object.
    bless $retVal, $class;
    return $retVal;
}

=head2 Public Manipulation Methods

=head3 CurateNewGenomes

    my $genomeMeta = $genomeLoader->CurateNewGenomes(\%genomeHash, $missingFlag, $clearFlag);

This method will check for redundant genomes in a set of incoming genomes and delete conflicting
genomes from the database. On exit, it will return a hash of the metadata for each nonredundant
genome.

The algorithm is complicated. We need to check for conflicts on genome ID and MD5 checksum,
and we need to give priority to core genomes. The basic steps are as follows.

=over 4

=item 1

Read in the ID, MD5, and type (core or not) for each genome in the database. If the clear-flag
is set, the database is presumed to be empty.

=item 2

Read in all the metadata for the incoming genomes. Sort them by type code (first core, then
non-core).

=item 3

Loop through the incoming genomes. If the genome has the same MD5 as a previously-seen input
genome, it is discarded. If it matches a database genome on ID or MD5 and both have the same
core status, it is discarded if the missing flag is set. Otherwise, it is discarded if the
database genome is a core genome.

=back

The parameters are as follows.

=over 4

=item genomeHash

Reference to a hash mapping each incoming genome ID to the source directory.

=item missingFlag

TRUE if existing genomes are preferred over new ones, else FALSE.

=item clearFlag

TRUE if the database is empty, else FALSE.

=item RETURN

Returns a hash keyed by genome ID that contains a metadata hash for each genome.

=back

=cut

sub CurateNewGenomes {
    # Get the parameters.
    my ($self, $genomeHash, $missingFlag, $clearFlag) = @_;
    # Get the loader object.
    my $loader = $self->{loader};
    # Get the statistics object.
    my $stats = $self->stats;
    # Get the database object.
    my $shrub = $self->db;
    # Get the data for all the genomes currently in the database. We will create two hashes, one keyed
    # by MD5 that lists genome IDs, and one keyed by genome ID that lists the MD5 and the core-flag.
    my %genomesById;
    my %genomesByMd5;
    # We only need to access the database if it has not been cleared.
    if (! $clearFlag) {
        print "Analyzing genomes currently in database.\n";
        my $q = $shrub->Get('Genome', '', []);
        while (my $genomeData = $q->Fetch()) {
            my ($id, $md5, $core) = $genomeData->Values([qw(id md5-identifier core)]);
            # Record the genome under its MD5.
            push @{$genomesByMd5{$md5}}, $id;
            # Record the genome's data.
            $genomesById{$id} = [$md5, $core];
            $stats->Add(existingGenomeRead => 1);
        }
    }
    # Get the metadata for each genome. We'll put it in this hash.
    my %retVal;
    # We will also build a list of genome IDs in here.
    my (@genomes, @nonCoreGenomes);
    print "Reading incoming genome metadata.\n";
    for my $genome (keys %$genomeHash) {
        # Get this genome's source directory.
        my $genomeDir = $genomeHash->{$genome};
        # Read the metadata.
        my $metaHash = $loader->ReadMetaData("$genomeDir/genome-info", required => [qw(name md5 type)]);
        # Is this a core genome?
        if ($metaHash->{type} eq 'c') {
            # Set the core flag to 1 (true) and store the genome in the core list.
            $metaHash->{type} = 1;
            push @genomes, $genome;
        } else {
            # Set the core flag to 0 (false) and store the genome in the non-core list.
            $metaHash->{type} = 0;
            push @nonCoreGenomes, $genome;
        }
        # Save the genome's metadata in the return hash.
        $retVal{$genome} = $metaHash;
    }
    # Get a list of all the incoming genomes, sorted core first.
    push @genomes, @nonCoreGenomes;
    # This will record the MD5s of genomes currently scheduled for addition.
    my %incomingMD5s;
    # Loop through the incoming genomes.
    for my $genome (@genomes) {
        # Get this genome's metadata and in particular its MD5.
        my $metaHash = $retVal{$genome};
        my $md5 = $metaHash->{md5};
        # if there is a previous incoming genome with the same MD5, discard this one.
        if ($incomingMD5s{$md5}) {
            $stats->Add(newGenomeConflict => 1);
            print "$genome discarded because of MD5 conflict with $incomingMD5s{$md5}.\n";
        } else {
            # Insure we don't load duplicates of this genome that come later in the list.
            $incomingMD5s{$md5} = 1;
            # Here we will build a list of genomes in the database that might conflict.
            my @rivals;
            # Check for an existing genome with the same ID.
            if ($genomesById{$genome}) {
                push @rivals, $genome;
            }
            # Check for existing genomes with the same MD5.
            if ($genomesByMd5{$md5}) {
                push @rivals, @{$genomesByMd5{$md5}};
            }
            # Loop through the rival genomes.
            my $discard;
            for my $rivalGenome (@rivals) {
                # Get the rival genome's core flag.
                my $rivalCore = $genomesById{$rivalGenome}[1];
                # Discard the new genome if it has the same core status as the rival and the MISSING
                # flag is set, or if the rival is a core genome. The net effect is that core genomes
                # always win, and if there is a tie, the missing-flag makes the decision.
                if ($rivalCore == $metaHash->{type} ? $missingFlag : $rivalCore) {
                    $discard = 1;
                }
            }
            if ($discard) {
                print "$genome skipped because of conflicts with existing genomes " . join(", ", @rivals) . "\n";
                $stats->Add(genomeConflictSkip => 1);
                # Remove the genome from the output hash.
                delete $retVal{$genome};
            } else {
                $stats->Add(genomeKept => 1);
                # Here we are keeping the genome. Delete the rivals.
                for my $rival (@rivals) {
                    print "Deleting genome $rival to make way for $genome.\n";
                    my $newStats = $shrub->Delete(Genome => $rival);
                    $stats->Accumulate($newStats);
                }
            }
        }
    }
    # Return the metadata hash.
    return \%retVal;
}

=head3 AnalyzeContigFasta

    my ($contigList, $genomeHash) = $genomeLoader->AnalyzeContigFasta($fileName);

Read and analyze the contig FASTA for a genome. This method computes the length, GC count, ID, and
MD5 for each contig in the FASTA file and returns the information in a list of hashes along with
a hash of global data for the genome.

=over 4

=item fileName

Name of the file containing the contig sequences in FASTA format.

=item RETURN

Returns a two-element list. The first element is list of hash references, one for each contig. Each hash
reference contains the following members.

=over 8

=item id

ID of the contig.

=item length

Total number of base pairs in the contig.

=item md5-identifier

MD5 digest of the contig sequence.

=back

The second element is a hash reference containing the following information about the genome.

=over 8

=item contigs

Number of contigs.

=item dna-size

Number of base pairs in all the contigs combined.

=item gc-conteent

Percent GC content present in the genome's DNA.

=item md5-identifier

MD5 identifier for this genome, used to determine if two genomes have the same DNA
sequence.

=back

=back

=cut

sub AnalyzeContigFasta {
    # Get the parameters.
    my ($self, $fileName) = @_;
    # Get the loader object.
    my $loader = $self->loader;
    # Get the statistics object.
    my $stats = $loader->stats;
    # Open the contig file.
    my $ih = $loader->OpenFile($fileName, 'contig');
    # Create the return variables.
    my (@contigList, %genomeHash);
    # Initialize the MD5 computer.
    $self->{md5} = MD5Computer->new();
    # Create the genome totals.
    my $gcCount = 0;
    my $dnaCount = 0;
    # Read the first line of the file.
    my $line = <$ih>;
    if (! defined $line) {
        # Here the contig file is empty. We are done, but we want to warn the user.
        print "Contig file $fileName is empty.\n";
        $stats->Add(emptyContigFile => 1);
    } elsif ($line !~ /^>(\S+)/) {
        # Here we have an invalid header.
        die "Invalid header in contig FASTA $fileName";
    } else {
        # Initialize the contig hash with the ID.
        my $contigHash = $self->_InitializeContig($1);
        $stats->Add(contigHeaders => 1);
        # Loop through the FASTA file.
        while (! eof $ih) {
            my $line = <$ih>;
            # Is this a contig header?
            if ($line =~ /^(\S+)/) {
                # Yes. Close the old contig and start a new one.
                my $contigID = $1;
                $self->_CloseContig($contigHash);
                push @contigList, $contigHash;
                $contigHash = $self->_InitializeContig($contigID);
                $stats->Add(contigHeaders => 1);
            } else {
                # No. Get the lengthand update the contig hash.
                chomp $line;
                my $len = length $line;
                $contigHash->{'length'} += $len;
                $dnaCount += $len;
                $stats->Add(dnaLetters => $len);
                # Accumulate the GC count.
                my $gc = ($line =~ tr/GCgc//);
                $gcCount += $gc;
                # Update the MD5 computation.
                $self->{md5}->AddChunk($line);
                $stats->Add(contigLine => 1);
            }
        }
        # Close off the last contig.
        $self->_CloseContig($contigHash);
        push @contigList, $contigHash;
    }
    # Compute the genome MD5.
    $genomeHash{'md5-identifier'} = $self->{md5}->CloseGenome();
    # Store the genome statistics.
    $genomeHash{'gc-content'} = $gcCount * 100 / $dnaCount;
    $genomeHash{contigs} = scalar @contigList;
    $genomeHash{'dna-size'} = $dnaCount;
    # Return the contig and genome info.
    return (\@contigList, \%genomeHash);
}

=head2 Internal Utility Methods

=head3 _InitializeContig

    my $contigHash = $genomeLoader->_InitializeContig($contigID);

This is a subroutine for L</AnalyzeContigFasta> that creates a new, blank contig hash for the
specified contig. It also starts the contig MD5 computation.

=over 4

=item contigID

ID of the new contig which is starting processing.

=item RETURN

Returns a reference to a hash containing the contig ID and spaces for the dna size and
MD5 identifier.

=back

=cut

sub _InitializeContig {
    # Get the parameters.
    my ($self, $contigID) = @_;
    # Create the return hash.
    my %retVal = (id => $contigID, 'md5-identifier' => "", 'length' => 0);
    # Start the MD5 computation.
    $self->{md5}->StartContig($contigID);
    # Return the new contig hash.
    return \%retVal;
}

=head3 _CloseContig

    $genomeLoader->_CloseContig(\%contigHash);

This is a subroutine for L</AnalyzeContigFasta> that completes the processing for a contig. The
MD5 computation is closed off and the MD5 identifier stored in the hash.

=over 4

=item contigHash

Reference to the hash containing the fields being computed for the current contig.

=back

=cut

sub _CloseContig {
    # Get the parameters.
    my ($self, $contigHash) = @_;
    # Close the MD5 computation.
    my $md5 = $self->{md5}->CloseContig();
    # Save the MD5 in the hash.
    $contigHash->{'md5-identifier'} = $md5;
}

1;
