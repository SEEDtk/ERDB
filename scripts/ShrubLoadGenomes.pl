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

=head1 Load Genomes Into the Shrub Database

    ShrubLoadGenomes [options] genome1 genome2 ...

This method loads one or more genomes from repository directories into the
Shrub database. The genome data will be assembled into load files for
each table, and then the tables loaded directly from the files.

=head2 Parameters

The positional parameters are names of the genomes to be loaded.

The command-line options listed in L<Shrub/script_options> are accepted
as well as the following.

=over 4

=item slow

Load the database with individual inserts instead of a table load.

=item missing

Load only genomes that are not already in the database.

=item clear

Re-create the tables before loading.

=item genomes

If specified, the name of a file containing a list of genome IDs. Genomes from this list will be loaded in
addition to any specified in the argument list. Mutually exclusive with C<all>.

=item override

If specified, new function assignments will overwrite existing function assignments. Otherwise, new
function assignments will be ignored.

=item all

Load all of the genomes in the genome directory. Mutually exclusive with C<genomes>.

=item genomeDir

Directory containing the genome source files. If not specified, the default will be
computed from information in the L<FIG_Config> module.

=back

=cut

    use strict;
    use Shrub;
    use ShrubLoader;
    use ShrubFunctionLoader;
    use ShrubGenomeLoader;
    use File::Path ();
    use ScriptUtils;

    # This is the list of tables we are loading.
    use constant LOADTABLES => qw(Genome Genome2Contig Contig Genome2Feature Feature
                                  Protein2Feature Protein Protein2Function
                                  Feature2Contig Feature2Function);

    # Start timing.
    my $startTime = time;
    $| = 1; # Prevent buffering on STDOUT.
    # Process the command line.
    my $opt = ScriptUtils::Opts('genomeDirectory genome1 genome2 ...', Shrub::script_options(),
            ["slow|s", "use individual inserts rather than table loads"],
            ["genomes=s", "name of a file containing a list of the genomes to load"],
            ["missing|m", "only load genomes not already in the database"],
            ["override|o", "override existing protein function assignments"],
            ["clear|c", "clear the genome tables before loading"],
            ["all|a", "process all genomes in the genome directory"],
            ["genomeDir|g=s", "genome directory containing the data to load", { default => "$FIG_Config::shrub_dir/Inputs/GenomeData" }]
        );
    # Connect to the database.
    print "Connecting to database.\n";
    my $shrub = Shrub->new_for_script($opt);
    # We are connected. Create the loader utility object.
    my $loader = ShrubLoader->new($shrub);
    # Create the genome loader utility object.
    my $genomeLoader = ShrubGenomeLoader->new($loader);
    # Get the statistics object.
    my $stats = $loader->stats;
    # Get the positional parameters.
    my @genomes = @ARGV;
    # Verify the genome directory.
    my $genomeDir = $opt->genomedir;
    if (! -d $genomeDir) {
        die "Invalid genome directory $genomeDir.";
    }
    # Get the list of genomes to load. We will store it in $genomeHash, as a hash mapping
    # genome IDs to [directory,name] pairs.
    print "Reading genome repository.\n";
    my $genomeHash = $loader->FindGenomeList($genomeDir);
    if ($opt->all) {
        if (scalar @genomes || $opt->genomes) {
            die "ALL option specified along with a list of genome IDs. Use one or the other.";
        }
    } else {
        # Here we are only doing some of the genomes. We'll put them in here.
        my $genomeList = [@genomes];
        # First, do we have a list file?
        if ($opt->genomes) {
            # Yes. Get the genomes in the list.
            my $genomeData = $loader->GetNamesFromFile(genome => $opt->genomes);
            push @$genomeList, @$genomeData;
        }
        # Now run through the genome list. If one of them is not in the repository, throw an error.
        # Otherwise, put it into a hash.
        my %genomeMap;
        for my $genome (@$genomeList) {
            my $genomeLoc = $genomeHash->{$genome};
            if (! $genomeLoc) {
                die "Genome $genome not found in repository.";
            } else {
                $genomeMap{$genome} = $genomeLoc;
            }
        }
        # Save the genome map.
        $genomeHash = \%genomeMap;
    }
    # Now "$genomeHash" contains a hash mapping the genomes we want to process to their directories and names.
    # We only need the directories, so we get rid of the names.
    for my $genome (keys %$genomeHash) {
        $genomeHash->{$genome} = $genomeHash->{$genome}[0];
    }
    print "Initializing function and role tables.\n";
    # Create the function loader utility object.
    my $funcLoader = ShrubFunctionLoader->new($loader, slow => $opt->slow);
    # Are we clearing?
    if ($opt->clear) {
        # Yes. The MISSING option is invalid.
        if ($opt->missing) {
            die "Cannot specify MISSING when CLEAR is used.";
        } else {
            print "CLEAR option specified.\n";
            $loader->Clear(LOADTABLES);
        }
    }
    # If we are NOT in slow mode, prepare the tables for loading.
    if (! $opt->slow) {
        $loader->Open(LOADTABLES);
    }
    # If we want to override function assignments, put the function relationships in replace mode.
    if ($opt->override) {
        $loader->ReplaceMode(qw(Feature2Function Protein2Function));
    }
    # The next step is to resolve collisions. The following method will check for duplicate genomes and
    # delete existing genomes that conflict with the incoming ones. At the end, $genomeMeta will be a
    # hash mapping the ID of each genome we need to process to its metadata.
    my $genomeMeta = $genomeLoader->CurateNewGenomes($genomeHash, $opt->missing, $opt->clear);
    # These variables will be used to display progress.
    my ($gCount, $gTotal) = (0, scalar(keys %$genomeMeta));
    # Get the DNA repository directory.
    my $dnaRepo = $shrub->DNArepo;
    # Loop through the incoming genomes.
    for my $genome (sort keys %$genomeMeta) {
         my $metaHash = $genomeMeta->{$genome};
         # Display our progress.
         $gCount++;
         print "Processing $genome ($gCount of $gTotal).\n";
         # Get the input repository directory.
         my $genomeLoc = $genomeHash->{$genome};
         # Form the repository directory for the DNA.
         my $relPath = $loader->RepoPath($metaHash->{name});
         my $absPath = "$dnaRepo/$relPath";
         if (! -d $absPath) {
             print "Creating directory $relPath for DNA file.\n";
             File::Path::make_path($absPath);
         }
         # Now we read the contig file and analyze the DNA for gc-content, number
         # of bases, and the list of contigs. We also copy it to the output
         # repository.
         print "Analyzing contigs.\n";
         my ($contigList, $genomeHash) = $genomeLoader->AnalyzeContigFasta("$genomeLoc/contigs", "$absPath/$genome.fa");
         # Get the annotation privilege level for this genome.
         my $priv = $metaHash->{privilege};
         # Now we can create the genome record.
         print "Storing $genome in database.\n";
         $loader->InsertObject('Genome', id => $genome, %$genomeHash,
                 core => $metaHash->{type}, name => $metaHash->{name}, prokaryotic => $metaHash->{prokaryotic},
                 'contig-file' => "$relPath/$genome.fa");
         $stats->Add(genomeInserted => 1);
         # Connect the contigs to it.
         for my $contigDatum (@$contigList) {
             # Fix the contig ID.
             $contigDatum->{id} = "$genome:$contigDatum->{id}";
             # Connect the genome to the contig.
             $loader->InsertObject('Genome2Contig', 'from-link' => $genome, 'to-link' => $contigDatum->{id});
             # Create the contig.
             $loader->InsertObject('Contig', %$contigDatum);
             $stats->Add(contigInserted => 1);
         }
         # Process the non-protein features.
         my $npFile = "$genomeLoc/non-peg-info";
         if (-f $npFile) {
             # Read the feature data.
             print "Processing non-protein features.\n";
             my $pegHash = $funcLoader->ReadFeatures($genome, $npFile);
             # Connect the functions.
             print "Connecting to functions.\n";
             for my $fid (keys %$pegHash) {
                 # Compute this function's ID.
                 my ($funcID, $comment) = @{$pegHash->{$fid}};
                 # Make the connection at each privilege level.
                 for (my $p = $priv; $p >= 0; $p--) {
                     $loader->InsertObject('Feature2Function', 'from-link' => $fid, 'to-link' => $funcID,
                             comment => $comment, security => $p);
                     $stats->Add(featureFunction => 1);
                 }
             }
         }
         print "Processing protein features.\n";
         my $pegHash = $funcLoader->ReadFeatures($genome, "$genomeLoc/peg-info");
         $funcLoader->ConnectPegFunctions($genome, $genomeLoc, $pegHash, priv => $priv);
     }
     # Unspool the load files.
     $loader->Close();
     # All done. Print the statistics.
     my $totalTime = time - $startTime;
     my $genomeCount = scalar keys %$genomeHash;
     if ($genomeCount > 0) {
         my $perGenome = ($totalTime / $genomeCount);
         print "$perGenome seconds per genome.\n";
     }
     $stats->Add(totalTime => $totalTime);
     print "All done.\n" . $stats->Show();
