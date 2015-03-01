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

use strict;
use Stats;
use SeedUtils;
use Shrub::DBLoader;
use Shrub;
use Shrub::SubsystemLoader;
use ScriptUtils;

=head1 Load Subsystems Into the Shrub Database

    ShrubLoadSubsystems [options] subsysDirectory

This script loads subsystems, related proteins, and their assignments into the Shrub database. The protein assignments
are taken from subsystems to insure they are of the highest quality. This process is used to prime
the database with priority assignments, and also to periodically update assignments.

This script performs three separate tasks-- loading the subsystem descriptors and the associated roles,
assigning functions to proteins, and connecting subsystems to genomes. Any or all of these functions can
be performed. The default is to do nothing (which is pretty useless). If you specify the C<all> command-line
option, all three functions will be performed. Otherwise, the individual functions can be turned on by
specifying the appropriate option (C<subs>, C<links>, and/or C<prots>).

=head2 Parameters

The positional parameter is the name of the directory containing the subsystem source
directory, which contains the source subsystem data in L<ExchangeFormat>. If omitted,
the directory name will be computed from information in the L<FIG_Config> module.

The command-line options are as specified in L<Shrub/script_options> plus
the following.

=over 4

=item subsystems

If specified, the name of a file containing subsystem names. Only the named subsystems
will be loaded. Otherwise, all the subsystems in the directory will be loaded.

=item missing

If specified, only missing subsystems will be loaded.

=item clear

If specified, the subsystem tables will be cleared prior to loading. If this is the case, C<missing>
will have no effect.

=item slow

If specified, each table record will be inserted individually. Otherwise, new records will be spooled to a
flat file and uploaded at the end of the run.

=item genomeDir

Directory containing the genome source data in L<ExchangeFormat>. If not specified, the default will be
computed from information in the L<FIG_Config> file.

=back

=cut

# Start timing.
my $startTime = time;
$| = 1; # Prevent buffering on STDOUT.
# Parse the command line.
my $opt = ScriptUtils::Opts('subsysDirectory', Shrub::script_options(),
        ["slow|s", "use individual inserts rather than table loads"],
        ["subsystems=s", "name of a file containing a list of the subsystems to use"],
        ["missing|m", "only load subsystems not already in the database"],
        ["clear|c", "clear the subsystem tables before loading", { implies => 'missing' }],
        ["genomeDir|g=s", "genome directory containing the data to load", { default => "$FIG_Config::data/Inputs/GenomeData" }]
    );
# Connect to the database.
print "Connecting to database.\n";
my $shrub = Shrub->new_for_script($opt);
# Get the positional parameters.
my ($subsysDirectory) = @ARGV;
# Get the genome directory.
my $genomeDirectory = $opt->genomedir;
# Validate the directories.
if (! -d $genomeDirectory) {
    die "Invalid genome directory $genomeDirectory.";
}
if (! $subsysDirectory) {
    $subsysDirectory = "$FIG_Config::data/Inputs/SubSystemData";
}
if (! -d $subsysDirectory) {
    die "Invalid subsystem directory $subsysDirectory.";
}
# Create the loader utility object.
my $loader = Shrub::DBLoader->new($shrub);
# Get the statistics object.
my $stats = $loader->stats;
# Create the subsystem loader utility object.
my $subLoader = Shrub::SubsystemLoader->new($loader, slow => $opt->slow);
# Now we need to get the list of subsystems to process.
my $subs;
if ($opt->subsystems) {
    # Here we have a subsystem list.
    $subs = $loader->GetNamesFromFile(subsystem => $opt->subsystems);
    print scalar(@$subs) . " subsystems read from " . $opt->subsystems . "\n";
} else {
    # Here we are processing all the subsystems in the subsystem directory.
    $subs = [ map { Shrub::NormalizedName($_) } grep { -d "$subsysDirectory/$_" } $loader->OpenDir($subsysDirectory, 1) ];
    print scalar(@$subs) . " subsystems found in repository at $subsysDirectory.\n";
}
# Are we clearing?
if ($opt->clear) {
    # Yes. Erase all the subsystem tables.
    print "CLEAR option specified.\n";
    $subLoader->Clear();
}
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
# Finally, this will be used to cache genome IDs.
my %genomes;
# Loop through the subsystems.
print "Processing the subsystem list.\n";
for my $sub (sort @$subs) {
    $stats->Add(subsystemCheck => 1);
    my $subDir = $loader->FindSubsystem($subsysDirectory, $sub);
    # This will be cleared if we decide to skip the subsystem.
    my $processSub = 1;
    # This will contain the subsystem's ID.
    my $subID;
    # We need to make sure the old version of the subsystem is gone. If we are clearing, it is
    # already gone. If we are in missing-mode, we skip the subsystem if it is
    # already there.
    if (! $opt->clear) {
        $subID = $loader->CheckByName('Subsystem', name => $sub, $subHash);
        if (! $subID) {
            # It's a new subsystem, so we have no worries.
            $stats->Add(subsystemAdded => 1);
        } elsif ($opt->missing) {
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
        $subID = $subLoader->LoadSubsystem($subID => $sub, $subDir, \%genomes);
    }
}
# Close and upload the load files.
print "Unspooling load files.\n";
$loader->Close();
# Compute the total time.
my $timer = time - $startTime;
$stats->Add(totalTime => $timer);
# Tell the user we're done.
print "Database processed.\n" . $stats->Show();

