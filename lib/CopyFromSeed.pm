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


package CopyFromSeed;

    use strict;
    use warnings;
    use base qw(RepoLoader);
    use File::Path;
    use File::Copy::Recursive;

=head1 CopyFromSeed Helper Object

This is a helper object that manages the data structures needed by L<CopyFromSeed.pl>.
Rather than pass dozens of parameters to each major subroutine, we simply pass around
this object. It contains the following fields.

=over 4

=item figDisk

Name of the SEED FIGdisk containing the source data for the copy.

=item opt

The L<Getopt::Long::Descriptive::Opts> object containing the command-line options.

=item genomesProcessed

A reference to a hash containing the ID of each genome that has been processed
for copying, either successfully or otherwise.

=item genomeNames

A reference to a hash mapping genome IDs to genome names.

=item genomeOutput

Name of the output directory for genomes, or C<undef> if genomes are not to
be copied.

=item subsysOutput

Name of the output directory for subsystems, or C<undef> if subsystems are
not to be copied.

=item subGenomes

TRUE if all genomes in a subsystem spreadsheet should be copied when the
subsystem is processed.

=item blacklist

Reference to a hash of genome IDs. Genomes in this list will never be copied.

=back

=head2 Command-Line Option Groups

=head3 subsys_options

    my @opt_specs = CopyFromSeed::subsys_options();

These are the command-line options relating to copying of subsystems.

=over 4

=item subsysDir

The path to the folder in which the exchange-format directories of
copied subsystems should be placed. The default is the default
subsystem input repository (C<$FIG_Config::shrub_dir/Inputs/SubSystemData>).

=item subsystems

If specified, the name of a file containing a list of the names
of the subsystems to copy. If omitted, all subsystems in the
specified SEED will be copied. If C<none>, no subsystems will
be copied.

=item subGenomes

If specified, all genomes in the spreadsheets of the specified subsystems
will be copied.

=item subpriv

If specified, the subsystems copied will be marked as privileged; that is,
only privileged users will be able to edit them.

=back

=cut

sub subsys_options {
    return (
            ["subsysDir|s=s", "output directory for subsystem folders", { default => "$FIG_Config::shrub_dir/Inputs/SubSystemData"}],
            ["subsystems=s", "file listing subsystems to copy (default all)"],
            ["subGenomes", "if specified, all genomes in the spreadsheets of the specified subsystems will be copied"],
            ["subpriv", "if specified, the subsystems copied will be treated as privileged"],
    );
}

=head3 genome_options

    my @opt_spec = CopyFromSeed::genome_options();

These are the command-line options relating to the copying of genomes.

=over 4

=item genomeDir

The path to the folder in which the exchange-format directories of copied
genomes should be placed. The default is the default genome input repository
(C<$FIG_Config::shrub_dir/Inputs/GenomeData>).

=item genomes

If specified, a file containing a list of the genome IDs for the genomes to
copy. If omitted, all genomes in the specified SEED will be copied. If
C<none>, no genomes will be copied.

=item blacklist

If specified, the name of a file containing a list of IDs for genomes that should
not be copied. This overrides all other parameters.

=item proks

If specified, only prokaryotic genomes will be copied.

=item genpriv

The privilege level of the genome data being copied-- 0 (public), 1 (projected), or
2 (privileged).

=back

=cut

sub genome_options {
    return (
            ["genomeDir|g=s", "output directory for genome folders", { default => "$FIG_Config::shrub_dir/Inputs/GenomeData"}],
            ["genomes=s", "file listing genomes to copy (default all)"],
            ["proks", "if specified, only prokaryotic genomes will be copied"],
            ["blacklist=s", "the name of a file containing IDs of genomes that should not be copied"],
            ["genpriv=i", "privilege level of the genomes being copied-- 0 (public), 1 (projected), or 2 (privileged)"],
    );
}

=head2 Special Methods

=head3 new

    my $helper = CopyFromSeed->new($opt, $figDisk);

Construct a new helper object with the specified command-line options and SEED FIGdisk.

=over 4

=item opt

L<Getopt::Long::Descriptive::Opts> object containing the command-line options.

=item figDisk

Name of the SEED FIGdisk directory from which the genome and subsystem data is being
copied.

=back

=cut

sub new {
    # Get the parameters.
    my ($class, $opt, $figDisk) = @_;
    # Create the base-class object.
    my $retVal = Loader::new($class);
    # Store the FIGdisk pointer and command-line options.
    $retVal->{opt} = $opt;
    $retVal->{figDisk} = $figDisk;
    # Validate the FIGdisk.
    if (! $figDisk) {
        die "A SEED FIGdisk location is required.";
    } elsif (! -d $figDisk) {
        die "SEED FIGdisk location $figDisk is invalid or not found.";
    } elsif (! -d "$figDisk/FIG/Data/Organisms" || ! -d "$figDisk/FIG/Data/Subsystems") {
        die "Directory $figDisk does not appear to be a FIGdisk directory.";
    }
    # Create the tracking hashes.
    $retVal->{genomesProcessed} = {};
    $retVal->{genomeNames} = {};
    # Get the output directories. We use hash notation rather than the member facility of $opt
    # in case the option was not defined by the client. (For example, a genome copy script would
    # not have "subsysDir".)
    my $genomeOption = $opt->{genomeDir};
    if (! $genomeOption || $genomeOption eq 'none') {
        $retVal->{genomeOutput} = undef;
    } else {
        $retVal->{genomeOutput} = $genomeOption;
    }
    my $subsysOption = $opt->{subsysdir};
    if (! $subsysOption || $subsysOption eq 'none') {
        $retVal->{subsysOutput} = undef;
    } else {
        $retVal->{subsysOutput} = $subsysOption;
    }
    # Determine if we are copying all the genomes for each processed subsystem.
    $retVal->{subGenomes} = ($retVal->{genomeOutput} && $opt->{subgenomes});
    # Check for a black list.
    if (! $retVal->{genomeOutput} || ! $opt->blacklist) {
        # Here there is no genome black list.
        $retVal->{blacklist} = {};
    } else {
        # Here we need to create a hash of the genome IDs in the blacklist file.
        $retVal->{blacklist} = { map { $_ => 1 } $retVal->GetNamesFromFile('blacklist-genome' => $opt->blacklist) };
    }
    # Return the created object.
    return $retVal;
}


=head2 Subsystem-Related Methods

=head3 ComputeSubsystems

    my $subList = $loader->ComputeSubsystems();

Compute the list of subsystems to process. The subsystem names will be
converted to directory format and directories that are not found will be
eliminated. This method should only be called

=cut

sub ComputeSubsystems {
    # Get the parameters.
    my ($self) = @_;
    # Declare the return variable.
    my @retVal;
    # Get the statistics object.
    my $stats = $self->stats;
    # Get the command-line options.
    my $opt = $self->{opt};
    # Compute the base subsystem directory.
    my $subBase = "$self->{figDisk}/FIG/Data/Subsystems";
    # Get the input list of subsystems.
    my $subFile = $opt->subsystems;
    my @inputSubs;
    if ($subFile) {
        # Get the list of subsystem names from the file.
        my $subList = $self->GetNamesFromFile(subsystem => $subFile);
        print scalar(@$subList) . " subsystem names read from $subFile.\n";
        # Insure all the subsystems exist.
        for my $sub (@inputSubs) {
            # Convert the subsystem name to a directory name.
            my $dirName = $sub;
            $dirName =~ tr/ /_/;
            # Verify the directory.
            if (! -d "$subBase/$sub") {
                print "Subsystem $sub not found in SEED.\n";
                $stats->Add(subsystemNotFound => 1);
            } elsif (! -f "$subBase/$sub/EXCHANGEABLE") {
                print "Subsystem $sub is private in SEED.\n";
                $stats->Add(subsystemPrivate => 1);
            } elsif (! -f "$subBase/$sub/spreadsheet") {
                # This is a real subsystem. Save it.
                push @retVal, $dirName;
                $stats->Add(subsystemKept => 1);
            }
        }
    } else {
        # Here we getting all subsystems. Read the directory.
        @retVal =
                grep { substr($_,0,1) ne '.' && -f "$subBase/$_/EXCHANGABLE" && -f "$subBase/$_/spreadsheet" } $self->OpenDir($subBase);
        $stats->Add(subsystemKept => scalar(@retVal));
        print scalar(@retVal) . " subsystems found in $subBase.\n";
    }
    # Return the result.
    return \@retVal;
}

=head3 CopySubsystem

    $loader->LoadSubsystem($sub);

Extract the specified subsystem from the SEED and place its
exchange-format files in the desired subsystem output directory.

=over 4

=item sub

The directory name of the subsystem to process. (This is essentially
the subsystem name with spaces converted to underscores.)

=back

=cut

sub CopySubsystem {
    # Get the parameters.
    my ($self, $sub) = @_;
    # Get the statistics object.
    my $stats = $self->stats;
    # Determine what we're doing with genomes. We may or may
    # not be loading them right now.
    my $subGenomeFlag = $self->{subGenomes};
    # Compute the input directory name.
    my $subDisk = "$self->{figDisk}/FIG/Data/Subsystems/$sub";
    print "Processing subsystem $sub.\n";
    $stats->Add(subsystemsAnalyzed => 1);
    # Open the spreadsheet file for input.
    my $ih = $self->OpenFile('spreadsheet-data' => "$subDisk/spreadsheet");
    # We'll put the output file handles in here if we're writing subsystems.
    my ($rh, $gh, $ph);
    # If we're writing, set up the output files.
    if ($self->{subsysOutput}) {
        my $outDir = "$self->{subsysOutput}/$sub";
        open($rh, ">$outDir/Roles") || die "Cannot open Roles output file: $!";
        open($gh, ">$outDir/GenomesInSubsys") || die "Cannot open GenomesInSubsys file: $!";
        open($ph, ">$outDir/PegsInSubsys") || die "Cannot open PegsInSubsys file: $!";
        # Now create the metafile. We start with the subsystem's privilege status.
        my %metaHash = ( privilege => $self->{opt}->subpriv );
        # Next read the version. If there is no version we default to 1.
        $metaHash{version} = ReadFlagFile("$subDisk/VERSION") // 1;
        # Now write the metafile.
        $self->WriteMetaData("$outDir/Info", \%metaHash);
        $stats->Add('subsystem-info' => 1);
    }
    # We'll store the list of role abbreviations in here.
    my @roleAbbrs;
    # Loop through the roles.
    my $done = 0;
    while (! eof $ih && ! $done) {
        my $roleData = $self->GetLine('spreadsheet-role' => $ih);
        if ($roleData->[0] eq "//") {
            # Here we've reached the end-of-section marker.
            $done = 1;
        } else {
            # Here we have a real role. Write it to the roles file.
            $self->PutLine('role', $rh, @$roleData);
        }
    }
    # Skip over the next two sections of the input file.
    my $marksLeft = 2;
    while (! eof $ih && $marksLeft) {
        my $line = <$ih>;
        $stats->Add('spreadsheet-skip-line' => 1);
        if (substr($line,0,2) eq '//') {
            $marksLeft--;
        }
    }
    # Now we're at the beginning of the genome section-- the true spreadsheet.
    my $rows = 0;
    while (! eof $ih) {
        my $row = $self->GetLine('spreadsheet-row' => $ih);
        my ($genome, $variant, @cells) = @$row;
        # Do we want to copy this genome?
        if ($subGenomeFlag) {
            # Yes. Copy it now.
            $self->CopyGenome($genome);
        }
        # Are we writing the subsystem?
        if ($gh) {
            # Yes. Check to see if we want to keep this variant.
            if ($variant =~ /\*?-1/) {
                # It's vacant, so we are skipping it.
                $stats->Add(vacantVariantSkipped => 1);
            } else {
                # This is a valid row. Try to get the genome name.
                my $genomeName = $self->GenomeName($genome);
                if (! $genomeName) {
                    # No genome name, so skip this genome.
                    print "Genome $genome not found in SEED-- subsystem row skipped.\n";
                    $stats->Add(genomeNotFound => 1);
                } else {
                    # We can actually write this row. Write the genome info.
                    $self->PutLine('subsystem-row' => $gh, $genome, $genomeName, $variant);
                    # Now loop through the cells.
                    for (my $i = 0; $i <= $#cells; $i++) {
                        my $cell = $cells[$i];
                        # Is there anything in this cell?
                        if (! $cell) {
                            # No, skip it.
                            $stats->Add(emptyCell => 1);
                        } else {
                            $stats->Add(fullCell => 1);
                            # Get this cell's features. We convert from a peg number to a full FIG ID.
                            # Note that if there is a period in the peg number, it is a non-peg and includes
                            # the feature type (e.g. "rna.4").
                            my @features = map { "fig|$genome." . (($_ =~ /\./) ? $_ : "peg.$_" ) } split /\s*,\s*/, $cell;
                            # Get the current cell's role abbreviation.
                            my $abbr = $roleAbbrs[$i];
                            # Output the PEG info.
                            for my $feature (@features) {
                                $self->PutLine('subsystem-feature' => $ph, $feature, $abbr);
                            }
                        }
                    }
                }
            }
        }
    }
}


=head3 CopyGenome

    $loader->CopyGenome($genome);

Extract a genome from the SEED and copy it to the output repository. If
the genome has already been copied, do nothing.

=over 4

=item genome

ID of the genome to copy.

=back

=cut

sub CopyGenome {
    # Get the parameters.
    my ($self, $genome) = @_;
    # Get the statistics object.
    my $stats = $self->stats;
    # Get the command-line options.
    my $opt = $self->{opt};
    # Get the genome output directory root.
    my $outRepo = $self->{genomeOutput};
    # Do the first-level filtering.
    if ($self->{genomesProcessed}{$genome}) {
        # Here we've already processed this genome, so we don't need
        # to look at it again.
        $stats->Add('genome-reprocessed' => 1);
    } elsif ($self->{blacklist}{$genome}) {
        # We are in the black list, so skip this genome.
        $stats->Add('blacklist-skip' => 1);
    } elsif (! $outRepo) {
        # We are not copying genomes.
        $stats->Add('genome-suppressed' => 1);
    } else {
        # Compute the genome input directory.
        my $genomeDir = "$self->{figDisk}/FIG/Data/Organisms/$genome";
        # Get the genome name.
        my $genomeName = $self->GenomeName($genome);
        if (! $genomeName) {
            # If we couldn't find the name, we can't process the genome.
            print "Could not find name of $genome-- skipped.\n";
            $stats->Add('genome-name-not-found' => 1);
        } elsif (! -d "$genomeDir/Features") {
            # If there are no features, we can't process the genome.
            print "$genome has no Feature directory.\n";
            $stats->Add('genome-no-features' => 1);
        } elsif (! -d "$genomeDir/Features/peg") {
            # If there are no pegs, we can't process the genome.
            print "$genome has no peg directory.\n";
            $stats->Add('genome-no-pegs' => 1);
        } elsif (! -f "$genomeDir/contigs") {
            # If there are no contigs, we can't process the genome.
            print "$genome has no contigs file.\n";
            $stats->Add('genome-no-contigs' => 1);
        } else {
            # Now find out if this genome is prokaryotic. If we don't know
            # for sure, then it isn't.
            my $taxonomy = $self->ReadFlagFile("$genomeDir/TAXONOMY");
            my $prokFlag = ($taxonomy && $taxonomy =~ /^Archaea|Bacteria/);
            if ($opt->proks && ! $prokFlag) {
                # Here we are only loading proks and this isn't one, so we
                # skip it.
                $stats->Add('non-prokaryotic-skipped' => 1);
            } else {
                # At this point we are ready to copy this genome. The first
                # step is to compute the output directory.
                print "Processing $genome.\n";
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
                # Create the path.
                my $outDir = join("/", $outRepo, $genus, $species, $genome);
                if (! -d $outDir) {
                    File::Path->make_path($outDir);
                    $stats->Add('genome-dir-created' => 1);
                }
                # Get the privilege level.
                my $privilege = $opt->genpriv;
                # We have almost all of the metadata. Now we want to copy the
                # contigs file and compute the MD5.
                my $md5 = $self->ProcessContigFile($genomeDir, $outDir);
                # Build the metahash.
                my %metaHash = (md5 => $md5, name => $genomeName, type => $privilege,
                        prokaryotic => $prokFlag);
                # Look for a taxonomy ID.
                my $taxID = $self->ReadFlagFile("$genomeDir/TAXONOMY_ID");
                if (defined $taxID) {
                    $metaHash{taxID} = $taxID;
                } else {
                    $stats->Add('taxid-not-found' => 1);
                }
                # We have all the metadata. Write the meta-file.
                $self->WriteMetaData("$genomeDir/genome-info", \%metaHash);
                # Load the functional assignments.
                my $funHash = $self->ReadFunctions($genomeDir);
                # Now create the peg-info file.
                open(my $ph, ">$outDir/peg-info") || die "Could not open peg-info for output: $!";
                # Read the peg features.
                $self->ProcessFeature($genomeDir, 'peg', $ph, $funHash);
                # Close the peg-info file.
                close $ph;
                # Now we want to copy the protein translations.
                my $inputFasta = "$genomeDir/Features/peg/fasta";
                if (! -f $inputFasta) {
                    # No FASTA file, so we'll leave the output file blank.
                    print "$genome has no PEG FASTA file.\n";
                    $self->CreateBlank('peg-trans' => "$outDir/peg-trans");
                } else {
                    # Here we can copy the FASTA file.
                    File::Copy::Recursive::fcopy($inputFasta, "$outDir/peg-trans");
                    $stats->Add('peg-fasta-copied' => 1);
                }
                # Create the non-peg-info file.
                open(my $nh, ">$outDir/non-peg-info") || die "Could not open non-peg-info for output: $!";
                # Now loop through the non-peg features.
                my @ftypes = grep { $_ ne 'peg' && substr($_,0,1) ne '.' && -d "$genomeDir/Features/$_" }
                        $self->OpenDir("$genomeDir/Features");
                for my $ftype (@ftypes) {
                    $stats->Add('non-peg-type' => 1);
                    $self->ProcessFeature($genomeDir, $ftype, $nh, $funHash);
                }
            }
        }
    }
    # Denote this genome has been processed so we don't look at it
    # again.
    $self->{genomesProcessed}{$genome} = 1;
}


=head3 ProcessFeature

    $self->ProcessFeature($genomeDir, $ftype, $oh, \%funHash);

Process the feature information for the specified feature type. The
deleted features are read first, then the feature table is read. The
feature table is used to produce feature information records in the
output. The functional assignments are taken from the incoming function
hash.

=over 4

=item genomeDir

Genome input directory.

=item ftype

Type of feature being processed.

=item oh

Open handle for writing the feature info records. Each record consists of
(0) a feature ID, (1) a location string, and (2) the functional assignment.

=item funHash

Reference to a hash mapping feature IDs to functional assignments.

=back

=cut

sub ProcessFeature {
    # Get the parameters.
    my ($self, $genomeDir, $ftype, $oh, $funHash) = @_;
    # Get the statistics object.
    my $stats = $self->stats;
    # Compute the input directory.
    my $inputDir = "$genomeDir/Features/$ftype";
    # The feature data will be stored in here and written later.
    # The hash will map feature IDs to location strings.
    my %fids;
    # Look for a deleted features file.
    my %deleted;
    if (-f "$inputDir/deleted.fids") {
        my $dels = $self->GetNamesFromFile("deleted-$ftype" => "$inputDir/deleted.fids");
        %deleted = map { $_ => } @$dels;
        $stats->Add(deletedFids => scalar keys %deleted);
    }
    # Is there a tbl file?
    if (-f "$inputDir/tbl") {
        # Yes. Open it for input.
        my $ih = $self->OpenFile("$ftype-data" => "$inputDir/tbl");
        # Loop through the file. Note we only keep non-deleted features.
        while (! eof $ih) {
            my $fidLine = $self->GetLine("$ftype-data" => $ih);
            my ($fid, $location) = @$fidLine;
            if ($deleted{$fid}) {
                $stats->Add("$ftype-delete-skip" => 1);
            } else {
                $fids{$fid} = $location;
            }
        }
    }
    # Merge the functions into the location map to produce the output.
    for my $fid (sort keys %fids) {
        my $function = $funHash->{$fid};
        if (! defined $function) {
            $stats->Add("$ftype-function-not-found" => 1);
            $function = '';
        } else {
            $self->PutLine("$ftype-line" => $oh, $fid, $fids{$fid}, $function);
        }
    }
}


=head3 ReadFunctions

    my $funHash = $loader->ReadFunctions($genomeDir);

Create a mapping of the feature IDs to functional assignments for the
specified genome input directory. The functional assignments can be in
any of three files-- C<assigned_functions>, C<proposed_no_ff_functions>,
or C<proposed_functions>. Each file overrides the previous one. If none
of the files exist, the returned hash will be empty, meaning every
feature in the genome will be considered hypothetical.

=over 4

=item genomeDir

SEED genome directory containing the input.

=item RETURN

Returns a reference to a hash mapping each feature to its functional
assignment.

=back

=cut

sub ReadFunctions {
    # Get the parameters.
    my ($self, $genomeDir) = @_;
    # Declare the return variable.
    my %retVal;
    # Loop through the three function files, in order.
    for my $file (qw(assigned_functions proposed_no_ff_functions proposed_functions)) {
        # Only proceed if the current file type exists.
        my $fileName = "$genomeDir/$file";
        if (-f $fileName) {
            my $ih = $self->OpenFile($file => $fileName);
            # Loop through this file. We blindly overwrite, so the last assignment always
            # wins.
            while (! eof $ih) {
                my $fidData = $self->GetLine($file => $ih);
                $retVal{$fidData->[0]} = $fidData->[1];
            }
        }
    }
    # Return the computed mapping.
    return \%retVal;
}


=head3 ComputeGenomes

    my $genomeList = $loader->ComputeGenomes();

Determine the list of genomes to copy. This could be all genomes in the
specified SEED or a list provided in a file.

=cut

sub ComputeGenomes {
    # Get the parameters.
    my ($self) = @_;
    # Get the statistics object.
    my $stats = $self->stats;
    # Declare the return variable.
    my $retVal;
    # Get the command-line options.
    my $opt = $self->{opt};
    # Check for a genome ID input file.
    if ($opt->genomes) {
        # We have one, so read the genomes from it.
        $retVal = $self->GetNamesFromList(genome => $opt->genomes);
        print scalar(@$retVal) . " genome IDs read from input file.\n";
    } else {
        # No genome input file, so read all the genomes in the genome
        # directory.
        my $orgDir = "$self->{figDisk}/FIG/Data/Organisms";
        $retVal = [ grep { $_ =~ /^\d+\.\d+$/ && -d "$orgDir/$_" } $self->OpenDir($orgDir) ];
        print scalar(@$retVal) . " genome IDs read from directory.\n";
    }
    # Return the resulting genome list.
    return $retVal;
}


=head3 ReadFlagFile

    my $data = CopyFromSeed::ReadFlagFile($fileName);

Read the data from a flag file. A flag file may or may not exist, and it always has a
single line of data in it.

=over 4

=item fileName

Name of the flag file to read.

=item RETURN

Returns the data from the file, or C<undef> if the file does not exist.

=back

=cut

sub ReadFlagFile {
    # Get the parameters.
    my ($fileName) = @_;
    # The return value will be put in here.
    my $retVal;
    # Only bother to read the file if it exists.
    if (-f $fileName) {
        open(my $ih, "<$fileName") || die "Could not open flag file $fileName: $!";
        $retVal = <$ih>;
        chomp $retVal;
    }
    # Return the data read (if any).
    return $retVal;
}

=head3 GenomeName

    my $genomeName = $self->GenomeName($genome);

Determine the name of the genome with the specified ID. This might
require reading it from the input organism directory or looking it up in
a hash.

=over 4

=item genome

ID of the genome whose name is desired.

=item RETURN

Returns the genome name, or C<undef> if the genome cannot be found.

=back

=cut

sub GenomeName {
    # Get the parameters.
    my ($self, $genome) = @_;
    # Get the genome name hash.
    my $nameHash = $self->{genomeNames};
    # Have we looked at this genome before? (NOTE we use
    # "exists" here because if the genome was not found
    # an undef will be stored for it in the name hash.)
    if (! exists $nameHash->{$genome}) {
        # No, we have to read its flag file. If the genome does not
        # exist, the flag file method will return undef, which will
        # be stored in the has.
        $nameHash->{$genome} = ReadFlagFile("$self->{figDisk}/FIG/Data/Organisms/$genome/GENOME");
        $self->stats->Add(genomeNameRead => 1);
    }
    # Return the result.
    return $nameHash->{$genome};
}

=head3 ProcessContigFile

    my $md5 = $loader->ProcessContigFile($genomeDir, $outDir);

Read the contigs file for a genome, compute its MD5, and copy it into the
output directory.

=over 4

=item genomeDir

SEED directory containing the input genome.

=item outDir

Output directory for the genome exchange files.

=item RETURN

Returns the MD5 identifying the genome's DNA sequence.

=back

=cut

sub ProcessContigFile {
    # Get the parameters.
    my ($self, $genomeDir, $outDir) = @_;
    # Get the statistics object.
    my $stats = $self->stats;
    # Open an MD5 computation object.
    my $md5Object = MD5Computer->new();
    # Open the contigs file for input.
    print "Reading contigs from $genomeDir.\n";
    my $ih = $self->OpenFile(contig => "$genomeDir/contigs");
    # Open the output contigs file.
    open(my $oh, ">$outDir/contigs") || die "Could not open contig output file in $outDir: $!";
    # Loop through the contigs input file.
    while (! eof $ih) {
        my $line = <$ih>;
        $stats->Add('contig-line-in' => 1);
        # Copy this line to the output.
        print $oh $line;
        $stats->Add('contig-line-out' => 1);
        # Is this a header?
        if ($line =~ /^>(\S+)/) {
            # Yes. Start a new contig.
            $md5Object->StartContig($1);
            $stats->Add('contig-header-line' => 1);
        } else {
            # No. Save the data.
            chomp $line;
            $md5Object->AddChunk($line);
            $stats->Add('contig-data-line' => 1);
        }
    }
    # Close the files.
    close $ih;
    close $oh;
    # Close the genome processing to get the MD5.
    my $retVal = $md5Object->CloseGenome();
    # Return it.
    return $retVal;
}


1;
