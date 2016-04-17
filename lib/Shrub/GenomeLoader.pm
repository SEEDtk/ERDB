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

package Shrub::GenomeLoader;

    use strict;
    use MD5Computer;
    use Shrub::Functions;
    use File::Path;
    use BasicLocation;

=head1 Shrub Genome Load Utilities

This package contains utilities for loading genomes. In particular, it contains methods for curating a
genome list to prevent duplicates and processing the genome contigs.

The object has the following fields.

=over 4

=item loader

L<Shrub::DBLoader> object for accessing the database and statistics.

=item md5

L<MD5Computer> object for computing genome and contig MD5s.

=item funcMgr

L<Shrub::Functions> object for processing functions and roles.

=item slow

TRUE if we are to load using individual inserts, FALSE if we are to load by spooling
inserts into files for mass loading.

=item exclusive

TRUE if we have exclusive access to the database, else FALSE. The default is FALSE.

=item dnaRepo

Path to the DNA repository. If an empty string, then DNA storage in the repository is
suppressed.

=item taxLoader

A L<Shrub::TaxonomyLoader> object specifying taxonomic data. This is used to compute the genome's
taxon ID.

=back

=cut

    # This is the list of tables we are loading.
    use constant LOAD_TABLES => qw(Role Function Function2Role Genome Contig Feature Protein
            Feature2Contig Feature2Function);


=head2 Special Methods

=head3 new

    my $genomeLoader = Shrub::GenomeLoader->new($loader, %options);

Construct a new, blank Shrub genome loader object.

=over 4

=item loader

L<Shrub::DBLoader> object to be used to access the database and the load utility methods.

=item options

A hash of options, including one or more of the following.

=over 8

=item slow

TRUE if we are to load using individual inserts, FALSE if we are to load by spooling
inserts into files for mass loading. The default is FALSE.

=item exclusive

TRUE if we have exclusive access to the database, else FALSE. The default is FALSE.

=item funcMgr

A L<Shrub::Functions> object for computing function and role IDs. If none is
provided, one will be created internally.

=item dnaRepo

Path to the DNA repository. If an empty string, then DNA storage in the repository is
suppressed. The default is the DNA repository value in L<FIG_Config>.

=item taxon

A L<Shrub::TaxonomyLoader> object containing taxonomic data. If omitted, taxonomic IDs are
not computed.

=back

=back

=cut

sub new {
    # Get the parameters.
    my ($class, $loader, %options) = @_;
    # Get the slow-load flag.
    my $slow = $options{slow} || 0;
    # Get the function-loader object.
    my $funcMgr = $options{funcMgr};
    # Get the DNA repository.
    my $shrub = $loader->db();
    my $dnaRepo = $options{dnaRepo} // $shrub->DNArepo('optional');
    # Get the taxonomy loader (if any).
    my $taxLoader = $options{taxon};
    # If the function loader was not provided, create one.
    if (! $funcMgr) {
        $funcMgr = Shrub::Functions->new($loader, exclusive => $options{exclusive});
    }
    # If we are NOT in slow-loading mode, prepare the tables for spooling.
    if (! $slow) {
        $loader->Open(LOAD_TABLES);
    }
    # Create the object.
    my $retVal = { loader => $loader, md5 => undef,
        funcMgr => $funcMgr, slow => $slow, dnaRepo => $dnaRepo,
        taxLoader => $taxLoader };
    # Bless and return the object.
    bless $retVal, $class;
    return $retVal;
}

=head2 Public Manipulation Methods

=head3 Clear

    $genomeLoader->Clear();

Recreate the genome-related tables.

=cut

sub Clear {
    # Get the parameters.
    my ($self) = @_;
    # Get the loader object.
    my $loader = $self->{loader};
    # CLear the tables.
    $loader->Clear(LOAD_TABLES);
}


=head3 ComputeGenomeList

    my $gHash = $genomeLoader->ComputeGenomeList($genomeDir, $genomeSpec);

Determine the genomes we want to load and return a hash mapping their IDs to their location
in the repository.

=over 4

=item genomeDir

Directory containing the genome repository.

=item genomeSpec

Either C<all>, indicating we want to load the entire repository, or the name of a tab-delimited file
whose first column contains the IDs of the genomes we want to load.

=item RETURN

Returns a reference to a hash that maps the ID of each genome we want to load to the directory
containing its exchange files.

=back

=cut

sub ComputeGenomeList {
    my ($self, $genomeDir, $genomeSpec) = @_;
    # Get the loader object.
    my $loader = $self->{loader};
    # Get the directory of the genome repository.
    my $retVal = $loader->FindGenomeList($genomeDir, nameless => 1);
    # Do we have a list file?
    if ($genomeSpec ne 'all') {
        # Yes. Get the genomes in the list.
        my $genomeList = $loader->GetNamesFromFile(genome => $genomeSpec);
        # Now run through the genome list. If one of them is not in the repository, throw an error.
        # Otherwise, put it into a hash.
        my %genomeMap;
        for my $genome (@$genomeList) {
            my $genomeLoc = $retVal->{$genome};
            if (! $genomeLoc) {
                die "Genome $genome not found in repository.";
            } else {
                $genomeMap{$genome} = $genomeLoc;
            }
        }
        # Save the genome map.
        $retVal = \%genomeMap;
    }
    # Return the computed genome hash.
    return $retVal;
}


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
    my $stats = $loader->stats;
    # Get the database object.
    my $shrub = $loader->db;
    # Get the data for all the genomes currently in the database. We will create two hashes, one keyed
    # by MD5 that lists genome IDs, and one keyed by genome ID that lists the MD5 and the core-flag.
    my %genomesById;
    my %genomesByMd5;
    # We only need to access the database if it has not been cleared.
    if (! $clearFlag) {
        print "Analyzing genomes currently in database.\n";
        my $q = $shrub->Get('Genome', '', [], 'id md5-identifier core');
        while (my $genomeData = $q->Fetch()) {
            my ($id, $md5, $core) = $genomeData->Values('id md5-identifier core');
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
        my $metaHash = $loader->ReadMetaData("$genomeDir/genome-info",
                required => [qw(name md5 privilege prokaryotic)]);
        # Is this a core genome?
        if ($metaHash->{privilege} eq Shrub::PRIV()) {
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
            # Delete the genome from the output hash.
            delete $retVal{$genome};
        } else {
            # Insure we don't load duplicates of this genome that come later in the list.
            # (This works because all the core genomes are first in the list.)
            $incomingMD5s{$md5} = $genome;
            # Here we will build a list of genomes in the database that might conflict.
            my %rivals;
            # Check for an existing genome with the same ID.
            if ($genomesById{$genome}) {
                $rivals{$genome} = 1;
            }
            # Check for existing genomes with the same MD5.
            if ($genomesByMd5{$md5}) {
                for my $rival (@{$genomesByMd5{$md5}}) {
                    $rivals{$rival} = 1;
                }
            }
            # Loop through the rival genomes.
            my $discard;
            for my $rivalGenome (keys %rivals) {
                # Get the rival genome's core flag.
                my $rivalCore = $genomesById{$rivalGenome}[1];
                # Discard the new genome if it has the same core status as the rival and the MISSING
                # flag is set, or if the rival is a core genome. The net effect is that core genomes
                # always win, and if there is a tie, the missing-flag makes the decision.
                if (($rivalCore == $metaHash->{type}) ? $missingFlag : $rivalCore) {
                    $discard = 1;
                }
            }
            if ($discard) {
                print "$genome skipped because of conflicts with existing genome(s) " . join(", ", keys %rivals) . "\n";
                $stats->Add(genomeConflictSkip => 1);
                # Remove the genome from the output hash.
                delete $retVal{$genome};
            } else {
                $stats->Add(genomeKept => 1);
                # Here we are keeping the genome. Delete the rivals.
                for my $rival (keys %rivals) {
                    if ($genome ne $rival) {
                        print "Deleting genome $rival to make way for $genome.\n";
                    } else {
                        print "Deleting old version of $genome.\n";
                    }
                    my $newStats = $shrub->Delete(Genome => $rival);
                    $stats->Accumulate($newStats);
                }
            }
        }
    }
    # Return the metadata hash.
    return \%retVal;
}

=head3 LoadGenome

    $genomeLoader->LoadGenome($genome, $genomeDir, $metaHash);

Load a genome into the database.Any previous copy of the genome must already have been deleted.
(This is done automatically by L</CurateNewGenomes>; otherwise, use the method L<ERDBtk/Dalete>).)

=over 4

=item genome

ID of the genome to load.

=item genomeDir

Directory containing the genome source files in L<ExchangeFormat>.

=item metaHash (optional)

A reference to a hash containing the metadata for the genome, mapping each
key to its value. If omitted, the metadata will be read from the C<genome-info>
file.

=back

=cut

sub LoadGenome {
    # Get the parameters.
    my ($self, $genome, $genomeDir, $metaHash) = @_;
    # Get the loader, shrub, and statistics objects.
    my $loader = $self->{loader};
    my $shrub = $loader->db;
    my $stats = $loader->stats;
    # Get the taxon loader (if any).
    my $taxLoader = $self->{taxLoader};
    # Get the function loader.
    my $funcMgr = $self->{funcMgr};
    # If we do not already have the metadata hash, read it in.
    if (! defined $metaHash) {
        $metaHash = $loader->ReadMetaData("$genomeDir/genome-info",
                required => [qw(name md5 privilege prokaryotic domain)]);
    }
     # Get the DNA repository directory.
     my $dnaRepo = $self->{dnaRepo};
     my $relPath = $loader->RepoPath($metaHash->{name});
     my $absPath;
     # Only proceed if this installation supports DNA.
     if ($dnaRepo) {
         # Form the repository directory for the DNA.
         $absPath = "$dnaRepo/$relPath";
         if (! -d $absPath) {
             print "Creating directory $relPath for DNA file.\n";
             File::Path::make_path($absPath);
         }
         $absPath .= "/$genome.fa";
     }
     $relPath .= "/$genome.fa";
     # Now we read the contig file and analyze the DNA for gc-content, number
     # of bases, and the list of contigs. We also copy it to the output
     # repository.
     print "Analyzing contigs.\n";
     my ($contigList, $genomeHash) = $self->AnalyzeContigFasta($genome, "$genomeDir/contigs", $absPath);
     # Get the annotation privilege level for this genome.
     my $priv = $metaHash->{privilege};
     # Compute the genetic code.
     my $code = $metaHash->{code} // 11;
     # Now we need to process the features. This hash holds the useful feature statistics, currently
     # only "longest-feature".
     my %fidStats = ('longest-feature' => 0);
     # Process the non-protein features.
     my $npFile = "$genomeDir/non-peg-info";
     if (-f $npFile) {
         # Read the feature data.
         print "Processing non-protein features.\n";
         $self->ReadFeatures($genome, $npFile, $priv, \%fidStats);
     }
     # Process the protein features.
     my $protHash = $self->ReadProteins($genome, $genomeDir);
     print "Processing protein features.\n";
     $self->ReadFeatures($genome, "$genomeDir/peg-info", $priv, \%fidStats, $protHash);
     # Compute the taxonomy ID for the genome.
     my ($taxID) = split /\./, $genome;
     my $conf = 0;
     if ($taxLoader) {
         ($conf, $taxID) = $taxLoader->ComputeTaxID($genome, $metaHash->{taxid}, $metaHash->{name});
     }
     # Now we can create the genome record.
     print "Storing $genome in database.\n";
     $loader->InsertObject('Genome', id => $genome, %$genomeHash,
             core => $metaHash->{type}, name => $metaHash->{name}, prokaryotic => $metaHash->{prokaryotic},
             'contig-file' => $relPath, 'genetic-code' => $code, domain => $metaHash->{domain}, %fidStats,
             Taxonomy2Genome_confidence => $conf, Taxonomy2Genome_link => $taxID);
     $stats->Add(genomeInserted => 1);
     # Connect the contigs to it.
     for my $contigDatum (@$contigList) {
         # Fix the contig ID.
         $contigDatum->{id} = RealContigID($genome, $contigDatum->{id});
         # Create the contig.
         $loader->InsertObject('Contig', %$contigDatum, Genome2Contig_link => $genome);
         $stats->Add(contigInserted => 1);
     }
}

=head3 ReadProteins

    my $protHash = $genomeLoader->ReadProteins($genome, $genomeDir);

Create a hash of the proteins in the specified FASTA file and insure they are in the
database.

=over 4

=item genome

ID of the genome whose protein file is to be read.

=item genomeDir

Directory containing the genome source files.

=item RETURN

Returns a reference to a hash mapping feature iDs to protein IDs. The proteins will have been
inserted into the database.

=back

=cut

sub ReadProteins {
    # Get the parameters.
    my ($self, $genome, $genomeDir) = @_;
    # Get the loader object.
    my $loader = $self->{loader};
    # Get the statistics object.
    my $stats = $loader->stats;
    # The return hash will go in here.
    my %retVal;
    # Open the genome's protein FASTA.
    print "Reading $genome proteins.\n";
    my $fh = $loader->OpenFasta(protein => "$genomeDir/peg-trans");
    # Loop through the proteins.
    while (my $protDatum = $loader->GetLine(protein => $fh)) {
        my ($pegId, undef, $seq) = @$protDatum;
        # Compute the protein ID.
        my $protID = Shrub::ProteinID($seq);
        # Insert the protein into the database.
        $loader->InsertObject('Protein', id => $protID, sequence => $seq);
        # Connect the protein to the feature in the hash.
        $retVal{$pegId} = $protID;
    }
    # Return the protein hash.
    return \%retVal;
}




=head3 AnalyzeContigFasta

    my ($contigList, $genomeHash) = $genomeLoader->AnalyzeContigFasta($genome, $inFile, $fileName);

Read and analyze the contig FASTA for a genome. This method computes the length, GC count, ID, and
MD5 for each contig in the FASTA file and returns the information in a list of hashes along with
a hash of global data for the genome. It also copies the contig file to the DNA repository.

=over 4

=item genome

ID of the relevant genome

=item inFile

Name of the file containing the contig sequences in FASTA format.

=item fileName

Output location in the DNA repository for the contig FASTA, or C<undef> if this installation has no
DNA repository.

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
    my ($self, $genome, $inFile, $fileName) = @_;
    # Get the loader object.
    my $loader = $self->{loader};
    # Get the statistics object.
    my $stats = $loader->stats;
    # Open the contig file.
    my $ih = $loader->OpenFile(contig => $inFile);
    # Open the output file.
    my $oh;
    if ($fileName) {
        open($oh, ">$fileName") || die "Could not open contig output file $fileName: $!";
        print "Writing contigs to $fileName.\n";
    }
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
        print "Contig file $inFile is empty.\n";
        $stats->Add(emptyContigFile => 1);
    } elsif ($line !~ /^>(\S+)/) {
        # Here we have an invalid header.
        die "Invalid header in contig FASTA $inFile";
    } else {
        # Compute the contig ID.
        my $contigID = RealContigID($genome, $1);
        # Write the real contig ID to the output.
        if ($oh) {
            print $oh ">$contigID\n";
        }
        # Initialize the contig hash with the ID.
        my $contigHash = $self->_InitializeContig($1);
        $stats->Add(contigHeaders => 1);
        # Loop through the FASTA file.
        while (! eof $ih) {
            # Read the next line.
            my $line = <$ih>;
            # Is this a contig header?
            if ($line =~ /^>(\S+)/) {
                # Yes. Close the old contig and start a new one.
                my $contigID = RealContigID($genome, $1);
                $self->_CloseContig($contigHash);
                push @contigList, $contigHash;
                $contigHash = $self->_InitializeContig($contigID);
                $stats->Add(contigHeaders => 1);
                # Write the new contig ID.
                if ($oh) {
                    print $oh ">$contigID\n";
                }
            } else {
                # No. Echo the output line.
                if ($oh) {
                    print $oh $line;
                }
                # Get the length and update the contig hash.
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

=head3 ReadFeatures

    $genomeLoader->ReadFeatures($genome, $fileName, $priv, \%fidStats, \%protHash);

Read the feature information from a tab-delimited feature file. For each feature, the file contains
the feature ID, its location string (Sapling format), and its functional assignment. This method
will insert the feature, connect it to the genome and the contig, then attach the functional
assignment.

=over 4

=item genome

ID of the genome whose feature file is being processed.

=item fileName

Name of the file containing the feature data to process.

=item priv

Privilege level for the functional assignments.

=item fidStats

A hash containing feature statistics stored in the Genome record, keyed by field name.

=item protHash (optional)

A hash mapping feature IDs to protein IDs.

=back

=cut

sub ReadFeatures {
    # Get the parameters.
    my ($self, $genome, $fileName, $priv, $fidStats, $protHash) = @_;
    # Get the loader object.
    my $loader = $self->{loader};
    # Get the function processor.
    my $funcMgr = $self->{funcMgr};
    # If no protein hash was provided, create an empty one.
    $protHash //= {};
    # This will track our progress.
    my $fcount = 0;
    # Get the statistics object.
    my $stats = $loader->stats;
    # Open the file for input.
    my $ih = $loader->OpenFile(feature => $fileName);
    # Loop through the feature file.
    while (my $featureDatum = $loader->GetLine(feature => $ih)) {
        # Get the feature elements.
        my ($fid, $locString, $function) = @$featureDatum;
        # Create a list of location objects from the location string.
        my @locs = map { BasicLocation->new($_) } split /\s*,\s*/, $locString;
        $stats->Add(featureLocs => scalar(@locs));
        # Compute the feature type.
        my $ftype;
        if ($fid =~ /fig\|\d+\.\d+\.(\w+)\.\d+/) {
            $ftype = $1;
        } else {
            die "Invalid feature ID $fid.";
        }
        # If this is NOT a peg and has no function, change the function to
        # the appropriate hypothetical.
        if ($ftype ne 'peg' && ! $function) {
            $function = "hypothetical $ftype";
        }
        # Compute the total sequence length.
        my $seqLen = 0;
        for my $loc (@locs) {
            $seqLen += $loc->Length;
        }
        # Merge the length into the feature statistics.
        if ($seqLen > $fidStats->{'longest-feature'}) {
            $fidStats->{'longest-feature'} = $seqLen;
        }
        # Compute the protein.
        my $protID = $protHash->{$fid} // '';
        # Compute the function checksum.
        my $md5 = Shrub::Checksum($function);
        # Connect the feature to the genome.
        $loader->InsertObject('Feature', id => $fid, 'feature-type' => $ftype,
                checksum => $md5, 'sequence-length' => $seqLen,
                Genome2Feature_link => $genome,
                Protein2Feature_link => $protID);
        $stats->Add(feature => 1);
        # Connect the feature to the contigs. This is where the location information figures in.
        my $ordinal = 0;
        for my $loc (@locs) {
            $loader->InsertObject('Feature2Contig', 'from-link' => $fid, 'to-link' => RealContigID($genome , $loc->Contig),
                    begin => $loc->Left, dir => $loc->Dir, len => $loc->Length, ordinal => ++$ordinal);
            $stats->Add(featureSegment => 1);
        }
        # Parse the function.
        my ($statement, $sep, $roles, $comment) = Shrub::Functions::Parse($function);
        # Insure it is in the database.
        my $funcID = $funcMgr->Process($statement, $sep, $roles);
        # Connect the functions. Make the connection at each privilege level.
        for (my $p = $priv; $p >= 0; $p--) {
            $loader->InsertObject('Feature2Function', 'from-link' => $fid, 'to-link' => $funcID,
                    comment => $comment, security => $p);
            $stats->Add(featureFunction => 1);
        }
        $fcount++;
    }
    print "$fcount features processed.\n";
}


=head3 RealContigID

    my $realContigID = GenomeLoader::RealContigID($genome, $contigID);

Convert a contig ID into a real contig ID with a genome ID attached. If
the genome ID is already attached, do not change anything.

=over 4

=item genome

Genome ID for this contig.

=item contigID

Internal ID for this contig.

=item RETURN

Returns a contig ID with the genome ID prefixed.

=back

=cut

sub RealContigID {
    # Get the parameters.
    my ($genome, $contigID) = @_;
    # Start with the internal contig ID.
    my $retVal = $contigID;
    # If there is no genome ID in it, add one.
    if ($retVal =~ /^[^:]+$/) {
        $retVal = "$genome:$contigID";
    }
    # Return the result.
    return $retVal;
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
