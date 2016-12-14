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
    use FIG_Config;
    use File::Path;
    use File::Copy::Recursive;
    use MD5Computer;
    use BasicLocation;
    use Shrub;

=head1 CopyFromSeed Helper Object

This is a helper object that manages the data structures needed by L<BuildRepo.pl>.
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

=item missing

If TRUE, genomes and subsystems already in the repository will not be recopied.

=item genomeIndex

A hash of existing genomes in the repository, mapping each genome ID to a 2-tuple
containing its name and directory locatino.

=item genomesOK

If TRUE, then genomes are being copied. If FALSE, genomes are turned off.

=item subsystemsOK

If TRUE, then subsystems are being copied. If FALSE, subsystems are turned off.

=item privilege

Privilege level of the incoming annotations.

=item subPriv

TRUE if the subsystems are core subsystems, else FALSE.

=item otherOutput

Name of the repo directory to contain global files unrelated to genomes or subsystems.

=back

=head2 Command-Line Option Groups

=head3 subsys_options

    my @opt_specs = CopyFromSeed::subsys_options();

These are the command-line options relating to copying of subsystems.

=over 4

=item subsystems

If specified, the name of a file containing a list of the names
of the subsystems to copy. If omitted or C<all>, all subsystems in the
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
            ["subsystems|subs=s", "file listing subsystems to copy (default all)", { default => 'all' }],
            ["subGenomes", "if specified, all genomes in the spreadsheets of the specified subsystems will be copied"],
            ["subpriv", "if specified, the subsystems copied will be treated as privileged"],
    );
}

=head3 genome_options

    my @opt_spec = CopyFromSeed::genome_options();

These are the command-line options relating to the copying of genomes.

=over 4

=item genomes

If specified, a file containing a list of the genome IDs for the genomes to
copy. If omitted or C<all>, all genomes in the specified SEED will be copied. If
C<none>, no genomes will be copied.

=item blacklist

If specified, the name of a file containing a list of IDs for genomes that should
not be copied. This overrides all other parameters.

=item proks

If specified, only prokaryotic genomes will be copied.

=back

=cut

sub genome_options {
    return (
            ["genomes=s", "file listing genomes to copy (default all)", { default => 'all' }],
            ["proks", "if specified, only prokaryotic genomes will be copied"],
    );
}

=head3 common_options

    my @opt_spec = CopyFromSeed::common_options();

These are command-line options common to both object types.

=over 4

=item repo

Directory containing an L<ExchangeFormat> repository for genome and subsystem data.

=item missing

If specified, only subsystems and/or genomes that do not already exist
in the repository will be copied. Normally, the existing copies will be
replaced by the new information.

=item privilege

The privilege level of the annotations-- 0 (public), 1 (projected), or
2 (privileged).

=item clear

Erase the target repository before copying.

=back

=cut

sub common_options {
    return (
            ['repo|r=s', "location of the target repository", { default => "$FIG_Config::data/Inputs" }],
            ["privilege=i", "privilege level of the annotations-- 0 (public), 1 (projected), or 2 (privileged)",
                    { default => Shrub::PUBLIC }],
            ["missing|m", "only copy missing subsystems and genomes"],
            ["clear", "erase the target repository before copying"],
            ["blacklist=s", "the name of a file containing IDs of genomes that should not be copied"],
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
    # Validate the FIGdisk, if one is specified.
    if ($figDisk) {
        CheckFigDisk($figDisk);
    }
    # Create the tracking hashes.
    $retVal->{genomesProcessed} = {};
    $retVal->{genomeNames} = {};
    # Get the genome output directory.
    my $repo = $opt->repo;
    my $genomeOption = "$repo/GenomeData";
    $retVal->{genomeOutput} = $genomeOption;
    # Clear the genome index.
    $retVal->{genomeIndex} = {};
    # Determine if we're loading genomes at all. Note that the genomes option may not exist,
    # so we have to use a hash reference on $opt instead of a member reference.
    $retVal->{genomesOK} = ($opt->{genomes} && $opt->{genomes} ne 'none');
    # Get the subsystem output directory.
    my $subsysOption = "$repo/SubSystemData";
    $retVal->{subsysOutput} = $subsysOption;
    # Determine if we're loading subsystems at all. Note that the subsystems option may not exist,
    # so we have to use a hash reference on $opt instead of a member reference.
    $retVal->{subsystemsOK} = ($opt->{subsystems} && $opt->{subsystems} ne 'none');
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
    # Get the global output directory.
    $retVal->{otherDir} = "$repo/Other";
    # Get the samples output directory.
    $retVal->{sampleDir} = "$repo/Samples";
    # Save the missing-flag and the privilege levels.
    $retVal->{missing} = $opt->missing;
    $retVal->{privilege} = $opt->privilege;
    $retVal->{subPriv} = $opt->{subpriv} // 0;
    # Return the created object.
    return $retVal;
}


=head2 Subsystem-Related Methods

=head3 subsys_repo

    my $dir = $loader->subsys_repo;

Return the directory name for the subsystem repository.

=cut

sub subsys_repo {
    return $_[0]->{subsysOutput};
}

=head3 ComputeSubsystems

    my $subList = $loader->ComputeSubsystems(\@subList);

Compute the list of subsystems to process. The subsystem names will be
converted to directory format and directories that are not found will be
eliminated.

=over 4

=item subList

Reference to a list of subsystem names. If undefined, all subsystems in the
current SEED will be processed.

=item RETURN

Returns a list of low-level directory names for the subsystems that
should be loaded.

=back

=cut

sub ComputeSubsystems {
    # Get the parameters.
    my ($self, $subList) = @_;
    # Declare the return variable.
    my @retVal;
    # Get the statistics object.
    my $stats = $self->stats;
    # Check the input list of subsystems.
    my @subs;
    if (! $subList) {
        # Here we getting all subsystems. Read the directory. We exclude hidden names (starts with .) and
        # names with control characters in them.
        my $subBase = "$self->{figDisk}/FIG/Data/Subsystems";
        @subs = grep { substr($_,0,1) ne '.' && $_ !~ /[\x00-\x1F]/ } $self->OpenDir($subBase);
    } else {
        # Here we are using the caller-specified list.
        push @subs, @$subList;
    }
    # Check each subsystem.
    for my $sub (@subs) {
        my $dir = $self->CheckSubsystem($sub);
        if ($dir) {
            push @retVal, $sub;
        }
    }
    print scalar(@retVal) . " subsystems selected for copying.\n";
    # Return the result.
    return \@retVal;
}

=head3 CheckSubsystem

    my $dirName = $loader->CheckSubsystem($subName);

Insure a subsystem name is valid. If it is, return its directory name in the SEED.

=over 4

=item subName

The name of the subsystem to check.

=item RETURN

If the subsystem name is valid, returns the base name of its directory in the
SEED. If the subsystem name is invalid or refers to a private or incomplete
subsystem, returns an undefined value.

=back

=cut

sub CheckSubsystem {
    # Get the parameters.
    my ($self, $subName) = @_;
    # Get the statistics object.
    my $stats = $self->stats;
    # This will be the return value.
    my $retVal;
    # Compute the base subsystem directory.
    my $subBase = "$self->{figDisk}/FIG/Data/Subsystems";
    # Convert the subsystem name to a directory name.
    my $dirName = $subName;
    $dirName =~ tr/ /_/;
    # Verify the directory.
    if (! -d "$subBase/$dirName") {
        print "Subsystem $subName not found in SEED.\n";
        $stats->Add(subsystemNotFound => 1);
    } elsif (! -f "$subBase/$dirName/EXCHANGABLE") {
        print "Subsystem $subName is private in SEED.\n";
        $stats->Add(subsystemPrivate => 1);
    } elsif (-f "$subBase/$dirName/spreadsheet") {
        # This is a real subsystem. Save it.
        $retVal = $dirName;
        $stats->Add(subsystemKept => 1);
    } else {
        print "Subsystem $subName has no spreadsheet.\n";
        $stats->Add(subsystemNoSheet => 1);
    }
    # Return the directory name.
    return $retVal;
}


=head3 CopySubsystem

    $loader->LoadSubsystem($sub);

Extract the specified subsystem from the SEED and place its
exchange-format files in the desired subsystem output directory.
This method should not be called if subsystem output is turned
off.

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
    # Get the command-line options.
    my $opt = $self->{opt};
    # Determine what we're doing with genomes. We may or may
    # not be loading them right now.
    my $subGenomeFlag = $self->{subGenomes};
    # Compute the input directory name.
    my $subDisk = "$self->{figDisk}/FIG/Data/Subsystems/$sub";
    print "Processing subsystem $sub.\n";
    $stats->Add(subsystemsAnalyzed => 1);
    # Open the spreadsheet file for input.
    my $ih = $self->OpenFile('spreadsheet-data' => "$subDisk/spreadsheet");
    # We'll put the output file handles in here.
    my ($rh, $gh, $ph);
    # Compute the output directory.
    my $dsub = RepoLoader::DenormalizedName($sub);
    my $outDir = "$self->{subsysOutput}/$dsub";
    # We'll set this to TRUE if we're skipping this subsystem.
    my $skip;
    # Insure the output directory exists.
    if (! -d $outDir) {
        print "Creating $outDir\n";
        File::Path::make_path($outDir);
        $stats->Add('subsystem-directory-created' => 1);
    } elsif ($self->{missing}) {
        # Here we would be copying over an existing subsystem.
        $skip = 1;
        print "$sub already in repository-- skipped.\n";
        $stats->Add('skip-sub-already-present' => 1);
    }
    if (! $skip) {
        # Here we can prepare for output.
        open($rh, ">$outDir/Roles") || die "Cannot open Roles output file: $!";
        open($gh, ">$outDir/GenomesInSubsys") || die "Cannot open GenomesInSubsys file: $!";
        open($ph, ">$outDir/PegsInSubsys") || die "Cannot open PegsInSubsys file: $!";
        # Now create the metafile. We start with the subsystem's privilege status.
        my %metaHash = ( privileged => $self->{subPriv},
                'row-privilege' => $self->{privilege} );
        # Next read the version. If there is no version we default to 1.
        $metaHash{version} = ReadFlagFile("$subDisk/VERSION") // 1;
        # Finally the classification.
        my $classes = ReadFlagFile("$subDisk/CLASSIFICATION");
        if ($classes) {
            $metaHash{class} = $classes;
        }
        # Now write the metafile.
        $self->WriteMetaData("$outDir/Info", \%metaHash);
        $stats->Add('subsystem-info' => 1);
        # We'll store the list of role abbreviations in here.
        my @roleAbbrs;
        # This will map abbreviations to roles.
        my %abbrMap;
        # This tracks the auxiliary roles.
        my %auxMap;
        # Loop through the roles.
        my $done = 0;
        while (! eof $ih && ! $done) {
            my $roleData = $self->GetLine('spreadsheet-role' => $ih);
            if ($roleData->[0] eq "//") {
                # Here we've reached the end-of-section marker.
                $done = 1;
            } else {
                # Here we have a real role. Save the abbreviation.
                push @roleAbbrs, $roleData->[0];
                $abbrMap{$roleData->[0]} = $roleData->[1];
            }
        }
        # Now we have the subsets. We want the aux-roles subset.
        my $marksLeft = 1;
        while (! eof $ih && $marksLeft) {
            my $row = $self->GetLine('spreadsheet-subset' => $ih);
            my ($sub, @idxes) = @$row;
            if (! $sub) {
                $stats->Add('blank-ss-line' => 1);
            } elsif ($sub eq '//') {
                $marksLeft--;
            } elsif ($sub =~ /^aux/) {
                # Here we have the auxiliary-role subset.
                for my $idx (@idxes) {
                    # Compute the role for this index.
                    my $abbr = $roleAbbrs[$idx - 1];
                    # Mark it as auxiliary.
                    $auxMap{$abbr} = 1;
                    $stats->Add('aux-role' => 1);
                }
            }
        }
        # Output the role lines.
        for my $abbr (@roleAbbrs) {
            my @roleData = ($abbr, $abbrMap{$abbr});
            if ($auxMap{$abbr}) {
                push @roleData, 'aux';
            }
            $self->PutLine('role', $rh, @roleData);
        }
        # At this point, we're at the beginning of the genome section-- the true spreadsheet.
        my $rows = 0;
        while (! eof $ih) {
            my $row = $self->GetLine('spreadsheet-row' => $ih);
            my ($genome, $variant, @cells) = @$row;
            $rows++;
            # Do we want to copy this genome?
            if ($subGenomeFlag) {
                # Yes. Copy it now.
                $self->CopyGenome($genome);
            }
            # Are we writing the subsystem?
            if ($gh) {
                # Yes. Check to see if we want to keep this variant.
                if ($variant =~ /^\*?-1/) {
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
                        $self->PutLine('subsystem-row' => $gh, $genome, $genomeName, $variant, $rows);
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
                                    $self->PutLine('subsystem-feature' => $ph, $feature, $abbr, $abbrMap{$abbr}, $rows);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}


=head2 Genome-Related Methods

=head3 genome_repo

    my $dir = $loader->genome_repo;

Return the directory name for the genome repository.

=cut

sub genome_repo {
    return $_[0]->{genomeOutput};
}

=head3 CopyGenome

    $loader->CopyGenome($genome, $dir);

Extract a genome from the SEED and copy it to the output repository. If
the genome has already been copied, do nothing.

=over 4

=item genome

ID of the genome to copy.

=item dir

If specified, the name of the directory from which to load the genome.

=back

=cut

sub CopyGenome {
    # Get the parameters.
    my ($self, $genome, $dir) = @_;
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
        my $genomeDir = $dir // "$self->{figDisk}/FIG/Data/Organisms/$genome";
        # Get the genome name.
        my $genomeName = $self->GenomeName($genome, $dir);
        if (! -d $genomeDir) {
            # Here the genome simply isn't in the SEED.
            print "Genome $genome not found in SEED-- skipped.\n";
            $stats->Add('genome-not-found' => 1);
        } elsif (! $genomeName) {
            # If we couldn't find the name, we can't process the genome.
            print "Could not find name of $genome-- skipped.\n";
            $stats->Add('genome-name-not-found' => 1);
        } elsif (! $dir && ! -f "$genomeDir/COMPLETE") {
            # We don't want incomplete genomes from real SEEDs.
            print "$genome is incomplete-- skipped.\n";
            $stats->Add('genome-incomplete' => 1);
        } elsif (! -d "$genomeDir/Features") {
            # If there are no features, we can't process the genome.
            print "$genome has no Features directory.\n";
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
            # Now find out this genome's domain.
            my $taxonomy = ReadFlagFile("$genomeDir/TAXONOMY");
            my $domain;
            if ($taxonomy) {
                ($domain) = split /\s*;\s*/, $taxonomy, 2;
            } else {
                $domain = 'unknown';
            }
            my $prokFlag = ($domain =~ /^(?:Archaea|Bacteria)/ ? 1 : 0);
            if ($opt->{proks} && ! $prokFlag) {
                # Here we are only loading proks and this isn't one, so we
                # skip it.
                $stats->Add('non-prokaryotic-skipped' => 1);
            } else {
                # Compute the genetic code.
                my $geneticCode = ReadFlagFile("$genomeDir/GENETIC_CODE");
                if (! $geneticCode) {
                    $stats->Add(geneticCodeDefaulted => 1);
                    $geneticCode = 11;
                    if ($domain eq 'Eukaryota') {
                        $geneticCode = 1;
                    } elsif ($genomeName =~ /^(?:Achole|Meso|Myco|Spiro|Urea)plasma/) {
                        $geneticCode = 4;
                    }
                }
                # Compute the output directory.
                my $relPath = $self->RepoPath($genomeName);
                # This will be set to TRUE if we are skipping this genome.
                my $skip;
                # Create the full path.
                my $outDir = join("/", $outRepo, $relPath, $genome);
                # Is there already a copy of the genome in the repository?
                my $genomeData = $self->{genomeIndex}{$genome};
                if ($genomeData) {
                    # Yes. Check the missing-flag.
                    if ($self->{missing}) {
                        # We are only copying missing genomes. Skip it.
                        print "$genome already in repository-- skipped.\n";
                        $stats->Add('genome-already-present-skipped' => 1);
                        $skip = 1;
                    } elsif ($genomeData->[0] ne $outDir) {
                        # It's here, but the path has changed. Delete the
                        # old copy.
                        my $oldDir = $genomeData->[0];
                        # We check to insure the repository index is not out of
                        # date, but if the old directory is there, we delete it.
                        if (-d $oldDir) {
                            print "Removing old copy of genome in $oldDir.\n";
                            File::Path::remove_tree($oldDir);
                        }
                    }
                }
                # Only proceed if we're not skipping.
                if (! $skip) {
                    print "Processing $genome.\n";
                    # Insure the output directory exists.
                    if (! -d $outDir) {
                        print "Creating directory $outDir.\n";
                        File::Path::make_path($outDir);
                        $stats->Add(genomeDirCreated => 1);
                    }
                    # Get the privilege level.
                    my $privilege = $self->{privilege};
                    # We have almost all of the metadata. Now we want to copy the
                    # contigs file and compute the MD5.
                    my $md5 = $self->ProcessContigFile($genomeDir, $outDir);
                    # Build the metahash.
                    my %metaHash = (md5 => $md5, name => $genomeName, privilege => $privilege,
                            prokaryotic => $prokFlag, domain => $domain, code => $geneticCode);
                    # Look for a taxonomy ID.
                    my $taxID = ReadFlagFile("$genomeDir/TAXONOMY_ID");
                    if (defined $taxID) {
                        $metaHash{taxID} = $taxID;
                    } else {
                        $stats->Add('taxid-not-found' => 1);
                    }
                    # We have all the metadata. Write the meta-file.
                    $self->WriteMetaData("$outDir/genome-info", \%metaHash);
                    # Load the functional assignments.
                    my $funHash = $self->ReadFunctions($genomeDir);
                    # Now create the peg-info file.
                    open(my $ph, ">$outDir/peg-info") || die "Could not open peg-info for output: $!";
                    # Read the peg features.
                    $self->ProcessFeatures($genomeDir, 'peg', $ph, $funHash);
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
                        $self->ProcessFeatures($genomeDir, $ftype, $nh, $funHash);
                    }
                }
            }
        }
    }
    # Denote this genome has been processed so we don't look at it
    # again.
    $self->{genomesProcessed}{$genome} = 1;
}


=head3 ProcessFeatures

    $self->ProcessFeatures($genomeDir, $ftype, $oh, \%funHash);

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

sub ProcessFeatures {
    # Get the parameters.
    my ($self, $genomeDir, $ftype, $oh, $funHash) = @_;
    print "Processing $ftype features.\n";
    # Get the statistics object.
    my $stats = $self->stats;
    # Compute the input directory.
    my $inputDir = "$genomeDir/Features/$ftype";
    # The feature data will be stored in here and written later.
    # The hash will map feature IDs to location strings.
    my %fids;
    # Look for a deleted features file.
    my %deleted;
    if (-f "$inputDir/deleted.features") {
        my $dels = $self->GetNamesFromFile("deleted-$ftype" => "$inputDir/deleted.features");
        %deleted = map { $_ => 1 } @$dels;
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
        # Get the functional assignment.
        my $function = $funHash->{$fid};
        if (! defined $function) {
            $stats->Add("$ftype-function-not-found" => 1);
            $function = '';
        }
        # We need to normalize the location strings. Convert them to location objects.
        my @locs = map { BasicLocation->new($_) } split /\s*,\s*/, $fids{$fid};
        my $locString = join(",", map { $_->String } @locs);
        # Output the ID, location, and function.
        $self->PutLine($ftype => $oh, $fid, $locString, $function);
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


=head3 AllGenomes

    my $genomeList = $loader->AllGenomes();

Return the list of all genomes in the current SEED.

=cut

sub AllGenomes {
    # Get the parameters.
    my ($self) = @_;
    # Read all the genomes in the organism directory.
    my $orgDir = "$self->{figDisk}/FIG/Data/Organisms";
    my $retVal = [ grep { $_ =~ /^\d+\.\d+$/ && -d "$orgDir/$_" } $self->OpenDir($orgDir) ];
    print scalar(@$retVal) . " genome IDs read from directory.\n";
    # Return the resulting genome list.
    return $retVal;
}

=head3 GenomeName

    my $genomeName = $self->GenomeName($genome, $dir);

Determine the name of the genome with the specified ID. This might
require reading it from the input organism directory or looking it up in
a hash.

=over 4

=item genome

ID of the genome whose name is desired.

=item dir (optional)

The directory containing the genome data.

=item RETURN

Returns the genome name, or C<undef> if the genome cannot be found.

=back

=cut

sub GenomeName {
    # Get the parameters.
    my ($self, $genome, $dir) = @_;
    # Get the genome name hash.
    my $nameHash = $self->{genomeNames};
    # Have we looked at this genome before? (NOTE we use
    # "exists" here because if the genome was not found
    # an undef will be stored for it in the name hash.)
    if (! exists $nameHash->{$genome}) {
        # No, we have to read its flag file. If the genome does not
        # exist, the flag file method will return undef.
        $dir //= "$self->{figDisk}/FIG/Data/Organisms/$genome";
        my $name = ReadFlagFile("$dir/GENOME");
        $self->stats->Add(genomeNameRead => 1);
        if (! $name) {
            # The genome is not in the input. Do we already have it in the repository?
            my $genomeData = $self->{genomeIndex}{$genome};
            if ($genomeData) {
                # Yes. Pull out its name.
                $name = $genomeData->[1];
                $self->stats->Add(genomeNameFromRepo => 1);
            }
        }
        # Store the name found.
        $nameHash->{$genome} = $name;
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


=head3 IndexGenomes

    $loader->IndexGenomes();

Build the index for the genome repository. The index relates each genome ID
to its relative location in the repository's directory tree.

This is the same as the base-class method L<RepoLoader/IndexGenomes>, except
in this case we know the directory name internally.

=cut
sub IndexGenomes {
    # Get the parameters.
    my ($self) = @_;
    # Do we have a genome repository?
    my $genomeDir = $self->{genomeOutput};
    if ($genomeDir) {
        # Yes. Call the base-class method.
        $self->RepoLoader::IndexGenomes($genomeDir);
    }
}


=head2 Utility Methods

=head3 taxRepo

    my $dir = $loader->taxRepo;

Return the name of the directory to contain the taxonomy data. If the directory does not exist, it will
be created.

=cut

sub taxRepo {
    my ($self) = @_;
    my $retVal = $self->{otherDir};
    if (! -d $retVal) {
        File::Copy::Recursive::pathmk($retVal);
    }
    return $self->{otherDir};
}

=head3 sampleRepo

    my $dir = $loader->sampleRepo;

Return the name of the directory to contain the sample data.

=cut

sub sampleRepo {
    my ($self) = @_;
    my $retVal = $self->{sampleDir};
    if (! -d $retVal) {
        File::Copy::Recursive::pathmk($retVal);
    }
    return $self->{sampleDir};
}


=head3 SetSEED

    $loader->SetSEED($figDisk, $privilege);

Set the SEED FIGdisk and the associated privilege levels.

=over 4

=item figDisk

Path to the SEED FIGdisk to be used as the source for subsequent loads.

=item privilege

The privilege level associated with the SEED's annotations and subsystems.

=back

=cut

sub SetSEED {
    # Get the parameters.
    my ($self, $figDisk, $privilege) = @_;
    # Get the statistics object.
    my $stats = $self->stats;
    # Verify the FIGdisk.
    CheckFigDisk($figDisk);
    # Store it in our data structures.
    $self->{figDisk} = $figDisk;
    $stats->Add(figDisks => 1);
    # Reset the statistics and hashes.
    $self->Reset($privilege);
}

=head3 Reset

    $loader->Reset($privilege);

Set up this object for another run through SEED genome directories.

=over 4

=item privilege

The privilege level associated with the forthcoming annotations and subsystems.

=back

=cut

sub Reset {
    my ($self, $privilege) = @_;
    # Get the statistics object.
    my $stats = $self->stats;
    # Store the privilege level.
    $self->{privilege} = $privilege;
    $self->{subPriv} = ($privilege == Shrub::PRIV ? 1 : 0);
    $stats->Add(coreSeeds => $self->{subPriv});
    # Refresh the genome index.
    $self->{genomeIndex} = $self->FindGenomeList($self->{genomeOutput});
    # Clear the tracking hashes.
    $self->{genomesProcessed} = {};
    $self->{genomeNames} = {};
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


=head3 CheckFigDisk

    CopyFromSeed::CheckFigDisk($figDisk);

Verify that a SEED FIGdisk directory name is valid.

=over 4

=item figDisk

Path of the proposed FIGdisk directory. This method will abort the program if it is not
a directory or does not contain valid subdirectories.

=back

=cut

sub CheckFigDisk {
    my ($figDisk) = @_;
    if (! -d $figDisk) {
        die "SEED FIGdisk location $figDisk is invalid or not found.";
    } elsif (! -d "$figDisk/FIG/Data/Organisms" || ! -d "$figDisk/FIG/Data/Subsystems") {
        die "Directory $figDisk does not appear to be a FIGdisk directory.";
    }
}

1;
