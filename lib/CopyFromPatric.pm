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


package CopyFromPatric;

    use strict;
    use warnings;
    use base qw(RepoLoader);
    use P3DataAPI;
    use File::Path;

=head1 Copy Genomes From Patric

This package is used to copy genomes from PATRIC into the SEEDtk genome repository. Because no subsystems are
involved, and because it does not support the legacy loading options, it is vastly simpler than L<CopyFromSeed>.
In particular, we load every genome we are given. There is no option to load all the genomes from a PATRIC
installation, so things like the blacklist or the prokaryote filter are unnecessary.

This object contains the following fields.

=over 4

=item opt

The L<Getopt::Long::Descriptive::Opts> object containing the command-line options.

=item genomeIndex

Reference to a hash of existing genomes in the repository, mapping each genomeID to a 2-tuple containing its name
and directory location.

=item genomesProcessed

Reference to a hash of genomes processed, mapping each genome ID to 1.

=item genomeOutput

Name of the output directory for genomes.

=item missing

If TRUE, genomes already in the repository will not be recopied.

=item privilege

Privilege level of the incoming annotations.

=item p3

L<P3DataAPI> object for talking to PATRIC.

=back

=head2 Special Methods

=head3 new

    my $loader = CopyFromPatric->new($privilege, $opt)

Create a new PATRIC genome loader with the specified command-line options.

=over 4

=item privilege

The privilege level to assign to annotations.

=item opt

L<Getopt::Long::Descriptive::Opts> object containing the command-line options, which should be those found in
L<CopyFromSeed/common_options>.

=back

=cut

sub new {
    my ($class, $privilege, $opt) = @_;
    # Create the base-class object.
    my $retVal = Loader::new($class);
    # Attach the command-line options.
    $retVal->{opt} = $opt;
    # Get the genome output directory.
    my $repo = $opt->repo;
    my $genomeOption = "$repo/GenomeData";
    $retVal->{genomeOutput} = $genomeOption;
    # Load the genome index.
    $retVal->{genomeIndex} = $retVal->FindGenomeList($genomeOption);
    # Denote no genomes have been processed.
    $retVal->{genomesProcessed} = {};
    # Set the missing-only and privilege options.
    $retVal->{missing} = $opt->missing;
    $retVal->{privilege} = $privilege;
    # Enable access to PATRIC from Argonne.
    $ENV{PERL_LWP_SSL_VERIFY_HOSTNAME} = 0;
    # Connect to PATRIC.
    $retVal->{p3} = P3DataAPI->new();
    # Bless and return the object.
    bless $retVal, $class;
    return $retVal;
}

=head2 Public Methods

=head3 CopyGenome

    $loader->CopyGenome($genome);

Copy the specified genome from PATRIC into the input repository.

=over 4

=item genome

ID of the genome to copy.

=back

=cut

sub CopyGenome {
    my ($self, $genome) = @_;
    # Get the statistics object.
    my $stats = $self->stats;
    # Get the genome output directory root.
    my $outRepo = $self->{genomeOutput};
    # Only proceed if this is a new genome for this run.
    if ($self->{genomesProcessed}{$genome}) {
        $stats->Add('genome-reprocessed' => 1);
    } else {
        # Get the genome from PATRIC.
        my $gto = $self->{p3}->gto_of($genome);
        if (! $gto) {
            print "Genome $genome not found in PATRIC-- skipped.\n";
            $stats->Add('genome-not-found-patric' => 1);
        } else {
            # This will be set to TRUE if we want to skip this genome.
            my $skip;
            # Get the genome name, domain, and genetic code.
            my $genomeName = $gto->{scientific_name};
            my $domain = $gto->{domain};
            my $geneticCode = $gto->{genetic_code};
            # Punt if the domain is missing.
            if (! $domain) {
                $skip = 1;
                print "WARNING: missing domain for $genome: $genomeName.\n";
                $stats->Add(missingDomainInPATRIC => 1);
            } else {
                # Clean up a bad domain name.
                $domain =~ s/\s//g;
                $domain = ucfirst $domain;
                # Check for contigs.
                my $contigs = $gto->{contigs};
                if (! $contigs || ! @$contigs) {
                    $skip = 1;
                    print "WARNING: missing contigs for $genome: $genomeName.\n";
                    $stats->Add(missingContigsInPATRIC => 1);
                }
            }
            # Determine if we are prokaryotic.
            my $prokFlag = ($domain eq 'Bacteria' || $domain eq 'Archaea');
            # Compute the output directory.
            my $relPath = $self->RepoPath($genomeName);
            # Create the full path.
            my $outDir = join("/", $outRepo, $relPath, $genome);
            # Is this genome already in the repo?
            my $genomeData = $self->{genomeIndex}{$genome};
            if ($genomeData) {
                # Yes. Check the missing-flag.
                if ($self->{missing}) {
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
                if (! -d $outDir) {
                    print "Creating directory $outDir.\n";
                    File::Path::make_path($outDir);
                    $stats->Add(genomeDirCreated => 1);
                }
                # Get the privilege level.
                my $privilege = $self->{privilege};
                # We have almost all of the metadata. Now we want to copy the
                # contigs file and compute the MD5.
                my $md5 = $self->ProcessContigs($gto, $outDir);
                # Build the metahash.
                my %metaHash = (md5 => $md5, name => $genomeName, privilege => $privilege,
                        prokaryotic => $prokFlag, domain => $domain, code => $geneticCode);
                # Get the taxonomy ID.
                my $taxID = $gto->{ncbi_taxonomy_id};
                if (defined $taxID) {
                    $metaHash{taxID} = $taxID;
                } else {
                    $stats->Add('taxid-not-found' => 1);
                }
                # We have all the metadata. Write the meta-file.
                $self->WriteMetaData("$outDir/genome-info", \%metaHash);
                # Create the peg data files.
                open(my $ph, ">$outDir/peg-info") || die "Cound not open peg-info for output: $!";
                open(my $fh, ">$outDir/peg-trans") || die "Count not open peg-trans for output: $!";
                # Process the features.
                $self->ProcessPegFeatures($gto, $ph, $fh);
                close $fh;
                close $ph;
                # Create the non-peg-info file.
                open(my $nh, ">$outDir/non-peg-info") || die "Could not open non-peg-info for output: $!";
                # Process the non-peg features.
                $self->ProcessNonPegFeatures($gto, $nh);
                close $nh;
            }
        }
    }
    # Denote this genome has been processed so we don't look at it
    # again.
    $self->{genomesProcessed}{$genome} = 1;
}

=head2 Internal Utility Methods

=head3 ProcessPegFeatures

    $loader->ProcessPegFeatures($gto, $ph, $fh);

Write the exchange data for the protein features in a genome.

=over 4

=item gto

L<GenomeTypeObject> for the genome whose features are to be output.

=item ph

Open output file handle for the C<peg-info> file.

=item fh

Open output file handle for the C<peg-trans> file.

=back

=cut

sub ProcessPegFeatures {
    my ($self, $gto, $ph, $fh) = @_;
    # Get the statistics object.
    my $stats = $self->stats;
    # Loop through the features.
    my $featureList = $gto->{features};
    for my $feature (@$featureList) {
        # Only process pegs.
        if ($feature->{type} eq 'CDS') {
            # Write the protein translation.
            print $fh ">$feature->{id}\n$feature->{protein_translation}\n";
            $stats->Add('peg-fasta-lineout' => 1);
            # Write the feature.
            $self->WriteFeatureData($feature, $ph);
        }
    }
}


=head3 ProcessNonPegFeatures

    $loader->ProcessNonPegFeatures($gto, $nh, $fh);

Write the exchange data for the non-protein features in a genome.

=over 4

=item gto

L<GenomeTypeObject> for the genome whose features are to be output.

=item ph

Open output file handle for the C<non-peg-info> file.

=back

=cut

sub ProcessNonPegFeatures {
    my ($self, $gto, $ph, $fh) = @_;
    # Get the statistics object.
    my $stats = $self->stats;
    # Loop through the features.
    my $featureList = $gto->{features};
    for my $feature (@$featureList) {
        # Only process non-pegs.
        if ($feature->{type} ne 'CDS') {
            # Write the feature.
            $self->WriteFeatureData($feature, $ph);
        }
    }
}


=head3 ProcessContigs

    my $md5 = $loader->ProcessContigs($gto);

Create the contig FASTA file for a genome and return its MD5.

=over 4

=item gto

L<GenomeTypeObject> for the genome whose contigs are to be processed.

=item outDir

Name of the output directory for the genome.

=item RETURN

Returns the MD5 for the genome's DNA.

=back

=cut

sub ProcessContigs {
    # Get the parameters.
    my ($self, $gto, $outDir) = @_;
    # Get the statistics object.
    my $stats = $self->stats;
    # Open an MD5 computation object.
    my $md5Object = MD5Computer->new();
    # Open the output contigs file.
    open(my $oh, ">$outDir/contigs") || die "Could not open contig output file in $outDir: $!";
    # Loop through the contigs.
    my $contigList = $gto->{contigs};
    for my $contig (@$contigList) {
        $stats->Add('contig-in' => 1);
        # Write the contig to the FASTA file.
        my ($id, $dna) = ($contig->{id}, $contig->{dna});
        print $oh ">$id\n$dna\n";
        $stats->Add('contig-line-out' => 2);
        # Record the contig for MD5.
        $md5Object->StartContig($id);
        $md5Object->AddChunk($dna);
    }
    # Close the file.
    close $oh;
    # Close the genome processing to get the MD5.
    my $retVal = $md5Object->CloseGenome();
    # Return it.
    return $retVal;
}


=head3 WriteFeatureData

    $loader->WriteFeatureData($feature, $oh);

Write the exchange data for a feature to the specified output.

=over 4

=item feature

I<feature> object (from a L<GenomeTypeObject>) for the feature to be written.

=item oh

Open file handle to which the feature data will be written.

=back

=cut

sub WriteFeatureData {
    my ($self, $feature, $oh) = @_;
    # Get the feature ID and function.
    my $id = $feature->{id};
    my $fun = $feature->{function};
    # Only proceed if we have a function.
    if (defined $fun) {
        # Get the location string.
        my @locs = map { BasicLocation->new($_) } @{$feature->{location}};
        my $locString = join(",", map { $_->String } @locs);
        # Write the feature line.
        $self->PutLine($feature->{type} => $oh, $id, $locString, $fun);
    }
}


1;