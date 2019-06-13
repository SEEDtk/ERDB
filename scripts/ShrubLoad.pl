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
use FIG_Config;
use Shrub;
use Shrub::DBLoader;
use ERDBtk::Utils;
use Shrub::GenomeLoader;
use Shrub::SubsystemLoader;
use Shrub::Functions;
use ScriptUtils;
use File::Copy::Recursive;
use Shrub::PostLoader;
use Shrub::ChemLoader;
use Shrub::TaxonomyLoader;

=head1 Shrub Database Loader

    ShrubLoad [ options ]

This is the general-purpose load script for the Shrub database. It handles both subsystem and genome
input repositories, and can initialize a database once it is created. The options are voluminous, but
its purpose is to allow initializing a complete database from a single script.

=head2 Parameters

The command-line options are those found in L<Shrub/script_options> and L<ERDBtk::Utils::init_options> plus
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

=item exclusive

If specified, it will be presumed we have exclusive access to the database, allowing
significant optimization. This is the default for localhost databases.

=item shared

If specified, it will be presumed we have only shared access to the database, requiring
greater care during operations. This is the default for remote databases.

=item tar

If specified, it will be presumed the input repository is stored in the specified C<tar.gz>
file.  Currently, packaged repository files have a root directory named C<Inputs>. This must
match the leaf directory name of the specified repository or nothing will work right.

=item maxgap

The maximum gap allowed between clustered features, in base pairs. The default is C<2000>.

=item noDNA

If specified, DNA will not be stored in the DNA repository.

=item nodomains

If specified, protein domains will not be loaded.

=item nochem

If specified, chemistry data will not be loaded.

=back

=cut

# Start timing.
my $startTime = time;
$| = 1; # Prevent buffering on STDOUT.
# Get the command parameters.
my $opt = ScriptUtils::Opts('', Shrub::script_options(), ERDBtk::Utils::init_options(),
        ['slow', "load using individual inserts instead of spooling to load files"],
        ['missing|m', "only load missing genomes and subsystems"],
        ['repo|r=s', "location of the input repository", { default => "$FIG_Config::data/Inputs" }],
        ['genomes=s', "file listing IDs of genomes to load, \"all\", or \"none\"", { default => 'all' }],
        ['subsystems|subs=s', "file listing IDs of subsystems to load, \"all\", or \"none\"", { default => 'all' }],
        ['tar=s', "file containing compressed copy of the input repository"],
        ['maxgap|g=i', "maximum gap allowed between clustered features", { default => 2000 }],
        ['noDNA', "suppress use of the DNA repository"],
        ['notaxon', "suppress loading the taxonomy data"],
        ['nodomains', "suppress loading the domains"],
        ['nochem', "suppress loading the chemistry data"],
        [xmode => [["exclusive|X", "exclusive database access"], ["shared|S", "shared database access"]]],
        );
# Find out what we are loading.
my $genomeSpec = $opt->genomes;
my $subsSpec = $opt->subsystems;
my $genomesLoading = ($genomeSpec ne 'none');
my $subsLoading = ($subsSpec ne 'none');
my $dnaFlag = ($opt->nodna ? 0 : 1);
# Validate the load specifications.
if ($genomesLoading && $genomeSpec ne 'all' && ! -f $genomeSpec) {
    die "Could not find genome list file $genomeSpec.";
} elsif ($subsLoading && $subsSpec ne 'all' && ! -f $subsSpec) {
    die "Could not find subsystem list file $subsSpec.";
}
# Get the input repository.
my $repo = $opt->repo;
if (! -d $repo) {
    die "Could not find main repository directory $repo.";
}
# We need to determine shared or exclusive mode. First, see if the user gave us
# explicit instructions.
my $xmode = $opt->xmode;
if (! defined $xmode) {
    # We need to compute the default. Find out where the database is.
    my $dbhost = $opt->dbhost // $FIG_Config::dbhost // '';
    $xmode = ($dbhost eq 'localhost' ? 'exclusive' : 'shared');
}
my $exclusive = ($xmode eq 'exclusive' ? 1 : 0);
print "Database access is $xmode.\n";
# Get the remaining options.
my $missingFlag = $opt->missing;
my $slowFlag = $opt->slow;
# Validate the mutually exclusive options.
if ($opt->clear && $missingFlag) {
    die "Cannot specify both \"clear\" and \"missing\".";
}
if ($opt->clear && ! $exclusive) {
    die "Cannot clear the database in shared mode.";
}
if (! $opt->notaxon && ! $exclusive) {
    die "\"notaxon\" is required in shared mode.";
}
# Now we are pretty sure we have good input, so we can start.
# Connect to the database. Note that we use the external DBD if "store" was specified.
print "Connecting to the database.\n";
my $shrub = Shrub->new_for_script($opt, externalDBD => $opt->store);
# Get the load helper.
my $loader = Shrub::DBLoader->new($shrub);
# Get the statistics object.
my $stats = $loader->stats;
# Create the ERDBtk utility object.'
my $utils = ERDBtk::Utils->new($shrub);
# Create the repository if necessary.
if ($opt->tar) {
    $loader->ExtractRepo($opt->tar, $repo);
}
# Get the list of questionable genomes.
my $questionablesFile = "$repo/Other/questionables.tbl";
my %questionables;
if (! -s $questionablesFile) {
    print "$questionablesFile missing or empty-- ignored.\n";
} else {
    open(my $ih, '<', $questionablesFile) || die "Could not open $questionablesFile: $!";
    while (! eof $ih) {
        my $line = <$ih>;
        if ($line =~ /^(\d+\.\d+)/) {
            $questionables{$1} = 1;
            $stats->Add(questionableGenome => 1);
        }
    }
}
# Create the post-loading helper object.
my $postLoader = Shrub::PostLoader->new($loader, maxGap => $opt->maxgap, exclusive => $exclusive);
# Compute the genome and subsystem repository locations.
my ($genomeDir, $subsDir) = map { "$repo/$_" } qw(GenomeData SubSystemData);
if ($genomesLoading && ! -d $genomeDir) {
    die "Could not find GenomeData in $repo.";
} elsif ($subsLoading && ! -d $subsDir) {
    die "Could not find SubSystemData in $repo.";
}
# Process the initialization options and remember if we cleared
# the database.
my $cleared = $utils->Init($opt);
# Merge the statistics.
$stats->Accumulate($utils->stats);
# If we're clearing, we need to erase the DNA repository.
my $dnaRepo = ($dnaFlag ? $shrub->DNArepo('optional') : '');
if ($cleared && $dnaRepo) {
    print "Erasing DNA repository.\n";
    File::Copy::Recursive::pathempty($dnaRepo) ||
        die "Error clearing DNA repository: $!";
}
# This hash will contain a list of genome IDs known to be in the database. The subsystem
# loader needs this information to process its row information.
my %genomes;
# Create the function and role loaders.
print "Analyzing functions and roles.\n";
my $roleMgr;
if ($genomesLoading || $subsLoading || ! $opt->nochem) {
    # Check for a role file.
    my $roleFile = "$repo/Other/roles.tbl";
    if (! -f $roleFile) {
        $roleFile = '';
    }
    # Create the role manager.
    $roleMgr = Shrub::Roles->new($loader, slow => $slowFlag, exclusive => $exclusive, roleFile => $roleFile);
}
# We only need the function loader if we are loading genomes.
my $funcMgr;
if ($genomesLoading) {
    $funcMgr = Shrub::Functions->new($loader, slow => $slowFlag, roles => $roleMgr,
            exclusive => $exclusive);
}
# These will hold the subsystem objects.
my ($sLoader, $subs);
if ($subsLoading) {
    print "Pre-loading roles for subsystems.\n";
    $sLoader = Shrub::SubsystemLoader->new($loader, roleMgr => $roleMgr, slow => $slowFlag,
            exclusive => $exclusive);
    # Get the list of subsystems to load.
    $subs = $sLoader->SelectSubsystems($subsSpec, $subsDir);
    # Pre-load the subsystem roles into the role manager. This insures that the subsystem version
    # of the role name takes precedence over later versions.
    for my $sub (sort @$subs) {
        print "Preloading roles for $sub.\n";
        # Open the role input file.
        my $subDir = $loader->FindSubsystem($subsDir, $sub);
        my $rh = $loader->OpenFile(role => "$subDir/Roles");
        # Loop through the roles.
        while (my $roleData = $loader->GetLine(role => $rh)) {
            # Get this role's description.
            my $role = $roleData->[1];
            # Compute the role ID. If the role is new, this inserts it in the database.
            my ($roleID) = $roleMgr->Process($role);
        }
    }
}
# This will track the genomes we load.
my $gHash;
# Here we process the taxonomies.
my $taxLoader;
if (! $opt->notaxon) {
    print "Initializing taxonomy loader.\n";
    $taxLoader = Shrub::TaxonomyLoader->new($loader);
    print "Loading taxonomy data.\n";
    $taxLoader->LoadTaxonomies($repo, slow => $slowFlag);
}
# Here we process the genomes.
if ($genomesLoading) {
    print "Processing genomes.\n";
    my $gLoader = Shrub::GenomeLoader->new($loader, funcMgr => $funcMgr, slow => $slowFlag,
            exclusive => $exclusive, dnaRepo => $dnaRepo, taxon => $taxLoader, qHash => \%questionables);
    # Determine the list of genomes to load.
    $gHash = $gLoader->ComputeGenomeList($genomeDir, $genomeSpec);
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
        my $ok = $gLoader->LoadGenome($genomeID, $gHash->{$genomeID}, $metaHash->{$genomeID}, $cleared);
        if ($ok) {
            $genomes{$genomeID} = 1;
        }
    }
}
# Here we process the subsystems.
if ($subsLoading) {
    print "Processing subsystems.\n";
    # We need to be able to tell which subsystems are already in the database. If the number of subsystems
    # being loaded is large, we spool all the subsystem IDs into memory to speed the checking process.
    my $subTotal = scalar @$subs;
    my $subHash;
    if ($subTotal > 200) {
        if ($opt->clear) {
            $subHash = {};
        } else {
            $subHash = { map { $_->[1] => $_->[0] } $shrub->GetAll('Subsystem', '', [], 'id name') };
        }
    }
    # Loop through the subsystems.
    print "Processing the subsystem list.\n";
    my $subCount = 0;
    for my $sub (sort @$subs) {
        $stats->Add(subsystemCheck => 1);
        $subCount++;
        print "Analyzing subsystem $sub ($subCount of $subTotal).\n";
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
# Load the chemistry data.
if (! $opt->nochem) {
    print "Processing chemistry data.\n";
    my $chemLoader = Shrub::ChemLoader->new($loader, exclusive => $exclusive, slow => $slowFlag,
            roleMgr => $roleMgr, repo => $repo);
    $chemLoader->Process();
}
# Close and upload the load files.
print "Unspooling load files.\n";
$loader->Close();
# This next section creates derived data and relies on the fact the database is already loaded.
if ($genomesLoading) {
    # We have new genomes, so process the clusters and protein families.
    my @ptables = qw(ProteinFamily Family2Protein);
    $loader->Open(qw(Cluster Cluster2Feature), @ptables);
    print "Creating clusters.\n";
    # Process the genomes.
    for my $genome (sort keys %$gHash) {
        my $name = $gHash->{$genome};
        print "Processing clusters for $genome: $name.\n";
        $postLoader->LoadClusters($genome);
    }
    # Update the function table for the universal roles.
    $postLoader->SetUniRoles();
    # Now the protein families.
    print "Processing protein families.\n";
    if (! $cleared) {
        $loader->Clear(@ptables);
    }
    # Process the protein families. First, we create the ProteinFamily records.
    my $ih = $loader->OpenFile(famFuns => "$repo/Other/famFuns.tbl");
    while (my $famData = $loader->GetLine(famFuns => $ih)) {
        my ($fam, $fun) = @$famData;
        # We need to compute the function ID.
        my ($statement, $sep, $roles) = $funcMgr->Parse($fun);
        my $funID = $funcMgr->Process($statement, $sep, $roles);
        # Create the family record.
        $loader->InsertObject('ProteinFamily', id => $fam, Function2Family_link => $funID);
    }
    # Now, create the protein/family links.
    close $ih;
    $ih = $loader->OpenFile(protFams => "$repo/Other/protFams.tbl");
    while (my $protData = $loader->GetLine(protFams => $ih)) {
        my ($prot, $fam) = @$protData;
        $loader->InsertObject('Family2Protein', 'from-link' => $fam, 'to-link' => $prot);
    }
    # Unspool the tables.
    print "Unspooling cluster and protein family tables.\n";
    $loader->Close();
}
# Next we must load the samples. These are always loaded in slow mode, with
# direct inserts. Genome and taxonomy data must already exist. If we have a samples directory, we
# ask the post-loader to load it.
if (-d "$repo/Samples") {
    $postLoader->LoadSamples("$repo/Samples");
}
# Finally, the domains. These are loaded from a global file.
# This will track the domains loaded.
my %domains;
if (! $opt->nodomains) {
    print "Processing domains.\n";
    # Set up to load the domain tables.
    my @dtables = qw(CddDomain Domain2Protein Domain2Role);
    if (! $cleared) {
        $loader->Clear(@dtables);
    }
    $loader->Open(@dtables);
    # There are used for input.
    my ($fields, $dh);
    # Process the role/domain file.
    print "Connecting domains to roles.\n";
    $dh = $loader->OpenFile(role_domains => "$repo/Other/roles_cdd.tbl");
    while ($fields = $loader->GetLine(role_domains => $dh)) {
        my ($roleID, $domains) = @$fields;
        my @domains = split /,/, $domains;
        for my $domain (@domains) {
            DomainCheck($domain);
            $loader->InsertObject('Domain2Role', 'from-link' => $domain, 'to-link' => $roleID);
        }
    }
    close $dh;
    # Process the protein/domain file.
    print "Connecting domains to proteins.\n";
    $dh = $loader->OpenFile(prot_domains => "$repo/Other/peg_md5_cdd.tbl");
    while ($fields = $loader->GetLine(prot_domains => $dh)) {
        my (undef, $prot, $domains) = @$fields;
        my @domains = split /;/, $domains;
        for my $domain (@domains) {
            DomainCheck($domain);
            $loader->InsertObject('Domain2Protein', 'from-link' => $domain, 'to-link' => $prot);
        }
    }
    # Unspool domains.
    $loader->Close();
}
# Compute the total time.
my $timer = time - $startTime;
$stats->Add(totalTime => $timer);
# Tell the user we're done.
print "All done.\n" . $stats->Show();

# Insure a CDD domain is in the database.
sub DomainCheck {
    my ($domain) = @_;
    if (! $domains{$domain}) {
        $loader->InsertObject('CddDomain', id => $domain);
        $domains{$domain} = 1;
        $stats->Add(domainNew => 1);
    } else {
        $stats->Add(domainAlreadyFound => 1);
    }
}

