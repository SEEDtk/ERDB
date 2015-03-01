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


use strict;
use warnings;
use Shrub;
use Shrub::DBLoader;
use ERDB::Utils;
use Shrub::GenomeLoader;
use Shrub::SubsystemLoader;
use Shrub::FunctionLoader;
use ScriptUtils;

=head1 Shrub Database Loader

    ShrubLoad [ options ]

This is the general-purpose load script for the Shrub database. It handles both subsystem and genome
input repositories, and can initialize a database once it is created. The options are voluminous, but
its purpose is to allow initializing a complete database from a single script.

=head2 Parameters

The command-line options are those found in L<Shrub/script_options> and L<ERDB::Utils::init_options> plus
the following.

=over 4

=item slow

Load the database with individual inserts instead of a table load.

=item missing

Load only genomes and subsystems that are not already in the database. This is mutually exclusive with
C<clear>.

=item repo

Directory containing an L<ExchangeFormat> repository of genome and subsystem data.

=item genomes

If specified, the name of a file containing a list of genome IDs. Genomes from this list will be
loaded. If C<all>, all genomes in the repo directory will be loaded. If C<none>, no genomes
will be loaded. The default is C<all>.

=item subsystems

If specified, the name of a file containing a list of subsystem names. Subsystems from this list
will be loaded. If C<all>, all subsystems in the repo directory will be loaded. If C<none>, no
subsystems will be loaded. The default is C<all>.

=back

=cut

# Start timing.
my $startTime = time;
$| = 1; # Prevent buffering on STDOUT.
# Get the command parameters.
my $opt = ScriptUtils::Opts('', Shrub::script_options(), ERDB::Utils::init_options(),
        ['slow', "load using individual inserts instead of spooling to load files"],
        ['missing|m', "only load missing genomes and subsystems"],
        ['repo|r', "location of the input repository", { default => "$FIG_Config::data/Inputs" }],
        ['genomes', "file listing IDs of genomes to load, \"all\", or \"none\"", { default => 'all' }],
        ['subsystems|subs', "file listing IDs of subsystems to load, \"all\", or \"none\"", { default => 'all' }],
    );
# Find out what we are loading.
my $genomeSpec = $opt->genomes;
my $subsSpec = $opt->subsystems;
my $genomesLoading = ($genomeSpec ne 'none');
my $subsLoading = ($subsSpec ne 'none');
# Compute the genome and subsystem repository locations.
my $repo = $opt->repo;
my ($genomeDir, $subsDir) = map { "$repo/$_" } qw(GenomeData SubSystemData);
if (! -d $repo) {
    die "Could not find main repository directory $repo.";
} elsif ($genomesLoading && ! -d $genomeDir) {
    die "Could not find GenomeData in $repo.";
} elsif ($subsLoading && ! -d $subsDir) {
    die "Could not find SubSystemData in $repo.";
}
# Validate the load specifications.
if ($genomesLoading && $genomeSpec ne 'all' && ! -f $genomeSpec) {
    die "Could not find genome list file $genomeSpec.";
} elsif ($subsLoading && $subsSpec ne 'all' && ! -f $subsSpec) {
    die "Could not find subsystem list file $subsSpec.";
}
# Get the remaining options.
my $missingFlag = $opt->missing;
my $slowFlag = $opt->slow;
# Validate the mutually exclusive options.
if ($opt->clear && $missingFlag) {
    die "Cannot specify both \"clear\" and \"missing\".";
}
# Now we are pretty sure we have good input, so we can start.
# Connect to the database. Note that we use the external DBD if "store" was specified.
print "Connecting to the database.\n";
my $shrub = Shrub->new_for_script($opt, externalDBD => $opt->store);
# Get the load helper.
my $loader = Shrub::DBLoader->new($shrub);
# Get the statistics object.
my $stats = $loader->stats;
# Create the ERDB utility object.'
my $utils = ERDB::Utils->new($shrub);
# Process the initialization options and remember if we cleared
# the database.
my $cleared = $utils->Init($opt);
# Merge the statistics.
$stats->Accumulate($utils->stats);
# This hash will contain a list of genome IDs known to be in the database. The subsystem
# loader needs this information to process its row information.
my %genomes;
# Create the function loader. Both the other loaders use it.
print "Analyzing functions and roles.\n";
my $funcLoader = Shrub::FunctionLoader->new($loader, rolesOnly => ! $genomesLoading, slow => $slowFlag);
# Here we process the genomes.
if ($genomesLoading) {
    print "Processing genomes.\n";
    my $gLoader = Shrub::GenomeLoader->new($loader, funcLoader => $funcLoader, slow => $slowFlag);
    # Determine the list of genomes to load.
    my $gHash = $gLoader->ComputeGenomeList($genomeDir, $genomeSpec);
    # Curate the genome list to eliminate redundant genomes. This returns a hash of genome IDs to
    # metadata for the genomes to load.
    my $metaHash = $gLoader->CurateNewGenomes($gHash, $missingFlag, $cleared);
    # Loop through the genomes, loading them.
    my @metaKeys = sort keys %$metaHash;
    my $gTotal = scalar @metaKeys;
    my $gCount = 0;
    for my $genomeID (@metaKeys) {
        $gCount++;
        $stats->Add(genomesProcessed => 1);
        print "Processing $genomeID ($gCount of $gTotal).\n";
        $loader->LoadGenome($genomeID, $gHash->{$genomeID}, $cleared);
        $genomes{$genomeID} = 1;
    }
}
# Here we process the subsystems.
if ($subsLoading) {
    print "Processing subsystems.\n";
    my $sLoader = Shrub::SubsystemLoader->new($loader, funcLoader => $funcLoader, slow => $slowFlag);
    # Get the list of subsystems to load.
    my $subs = $sLoader->SelectSubsystems($subsSpec, $subsDir);
    # We need to be able to tell which subsystems are already in the database. If the number of subsystems
    # being loaded is large, we spool all the subsystem IDs into memory to speed the checking process.
    my $subHash;
    if (scalar @$subs > 200) {
        if ($opt->clear) {
            $subHash = {};
        } else {
            $subHash = { map { $_->[1] => $_->[0] } $shrub->GetAll('Subsystem', '', [], 'id name') };
        }
    }
    # Loop through the subsystems.
    print "Processing the subsystem list.\n";
    for my $sub (sort @$subs) {
        $stats->Add(subsystemCheck => 1);
        my $subDir = $loader->FindSubsystem($subsDir, $sub);
        # This will be cleared if we decide to skip the subsystem.
        my $processSub = 1;
        # This will contain the subsystem's ID.
        my $subID;
        # We need to make sure the old version of the subsystem is gone. If we are clearing, it is
        # already gone. If we are in missing-mode, we skip the subsystem if it is
        # already there.
        if (! $cleared) {
            $subID = $loader->CheckByName('Subsystem', name => $sub, $subHash);
            if (! $subID) {
                # It's a new subsystem, so we have no worries.
                $stats->Add(subsystemAdded => 1);
            } elsif ($missingFlag) {
                # It's an existing subsystem, but we are skipping existing subsystems.
                print "Subsystem \"$sub\" already in database-- skipped.\n";
                $stats->Add(subsystemSkipped => 1);
                $processSub = 0;
            } else {
                # Here we must delete the subsystem. Note we still have the ID.
                print "Deleting existing copy of $sub.\n";
                my $delStats = $shrub->Delete(Subsystem => $subID);
                $stats->Accumulate($delStats);
                $stats->Add(subsystemReplaced => 1);
            }
        }
        if ($processSub) {
            # Now the old subsystem is gone. We must load the new one. If we don't have an ID yet, it
            # will be computed here.
            $subID = $sLoader->LoadSubsystem($subID => $sub, $subDir, \%genomes);
        }
    }
}
# Close and upload the load files.
print "Unspooling load files.\n";
$loader->Close();
# Compute the total time.
my $timer = time - $startTime;
$stats->Add(totalTime => $timer);
# Tell the user we're done.
print "Database loaded.\n" . $stats->Show();
