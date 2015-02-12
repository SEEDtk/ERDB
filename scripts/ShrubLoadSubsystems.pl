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
    use ShrubLoader;
    use Shrub;
    use ShrubFunctionLoader;

=head1 Load Subsystems Into the Shrub Database

    ShrubLoadSubsystems.pl [options] subsysDirectory

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
directory. If omitted, it will be computed from information in the L<FIG_Config> module.

The command-line options are as specified in L<Shrub/new_for_script> plus
the following.

=over 4

=item privilege

Privilege level (0, 1, or 2). The function assignments will be added at the specified privilege
level and all levels below it and the subsystems are given the specified privilege level. The default
is C<0>, indicating unprivileged subsystems and assignments.

=item subsystems

If specified, the name of a file containing subsystem names. Only the named subsystems
will be loaded. Otherwise, all the subsystems in the directory will be loaded.

=item missing

If specified, only missing subsystems will be loaded.

=item clear

If specified, the subsystem tables will be cleared prior to loading. If this is the case, C<missing>
will have no effect.

=item links

If specified, the subsystem-to-genome links will be filled in.

=item prots

If specified, the proteins and function assignments will be filled in.

=item roles

If specified, the subsystems and roles will be filled in.

=item slow

If specified, each table record will be inserted individually. Otherwise, new records will be spooled to a
flat file and uploaded at the end of the run.

=item all

Implies C<links>, C<prots>, and C<roles>.

=item genomeDir

Directory containing the genome source files. If not specified, the default will be
computed from information in the L<FIG_Config> file.

=back

=cut

    # Start timing.
    my $startTime = time;
    $| = 1; # Prevent buffering on STDOUT.
    # Connect to the database.
    print "Connecting to database.\n";
    my ($shrub, $opt) = Shrub->new_for_script('%c %o subsysDirectory', { },
            ["privilege|p=i", "privilege level for assignment", { default => 0 }],
            ["slow|s", "use individual inserts rather than table loads"],
            ["subsystems=s", "name of a file containing a list of the subsystems to use"],
            ["missing|m", "only load subsystems not already in the database"],
            ["clear|c", "clear the subsystem tables before loading", { implies => 'missing' }],
            ["links|L", "process subsystem/genome links"],
            ["prots|P", "process proteins and functional assignments"],
            ["roles|R", "process subsystems and roles"],
            ["all|A", "process all functions (same as LPS)", { implies => ['links', 'prots', 'roles'] }],
            ["genomeDir|g", "genome directory containing the data to load", { default => "$FIG_Config::shrub_dir/Inputs/GenomeData" }]
        );
    # Get the positional parameters.
    my ($subsysDirectory) = @ARGV;
    # Get the genome directory.
    my $genomeDirectory = $opt->genomedir;
    # Validate the directories.
    if (! -d $genomeDirectory) {
        die "Invalid genome directory $genomeDirectory.";
    }
    if (! $subsysDirectory) {
        $subsysDirectory = "$FIG_Config::shrub_dir/Inputs/SubSystemData";
    }
    if (! -d $subsysDirectory) {
        die "Invalid subsystem directory $subsysDirectory.";
    }
    # Validate the mutually exclusive options.
    if (! $opt->roles && $opt->missing) {
        die "Option \"roles\" or \"all\" is required when missing-mode or clearing is requested.";
    }
    # Create the loader utility object.
    my $loader = ShrubLoader->new($shrub);
    # Get the statistics object.
    my $stats = $loader->stats;
    print "Initializing function and role tables.\n";
    # Create the function loader utility object.
    my $funcLoader = ShrubFunctionLoader->new($loader);
    # Extract the privilege level.
    my $priv = $opt->privilege;
    if ($priv > Shrub::MAX_PRIVILEGE || $priv < 0) {
        die "Invalid privilege level $priv.";
    }
    print "Privilege level is $priv.\n";
    # Now we need to get the list of subsystems to process.
    my $subs;
    if ($opt->subsystems) {
        # Here we have a subsystem list.
        $subs = $loader->GetNamesFromFile($opt->subsystems, "subsystem");
        print scalar(@$subs) . " subsystems read from " . $opt->subsystems . "\n";
    } else {
        # Here we are processing all the subsystems in the subsystem directory.
        $subs = [ map { Shrub::SubsystemID($_) } grep { -d "$subsysDirectory/$_" } ShrubLoader::OpenDir($subsysDirectory, 1) ];
        print scalar(@$subs) . " subsystems found in repository at $subsysDirectory.\n";
    }
    # Are we clearing?
    if ($opt->clear) {
        # Yes. Erase all the subsystem tables.
        print "CLEAR option specified.\n";
        $loader->Clear(qw(Subsystem Subsystem2Genome Subsystem2Role));
    }
    # Set up the tables we are going to load. Duplicate proteins will be discarded, but new function
    # assignments will replace old ones. Old subsystem data will be deleted before the new data is
    # loading. We only set up the tables in normal mode. In slow mode, our failure to do so will
    # cause the loader package to do manual inserts.
    if (! $opt->slow) {
        $loader->Open(qw(Protein Protein2Function Subsystem Subsystem2Genome Subsystem2Role));
    }
    $loader->ReplaceMode('Protein2Function');
    # We need to be able to tell which subsystems are already in the database. If the number of subsystems
    # being loaded is large, we spool all the subsystem IDs into memory to speed the checking process.
    my $subHash;
    if (scalar @$subs > 200) {
        if ($opt->clear) {
            $subHash = {};
        } else {
            $subHash = { map { $_ => 1 } $shrub->GetFlat('Subsystem', '', [], 'id') };
        }
    }
    # Process the subsystems one at a time in phases. First we load the subsystem proper and the roles.
    # If we are not doing roles, we note which subsystems we have to skip because they do not exist.
    # The subsystems we keep will go in this hash, which maps the names to directories.
    my %subDirs;
    print "Processing the subsystem list.\n";
    for my $sub (sort @$subs) {
        $stats->Add(subsystemCheck => 1);
        my $subDir = ShrubLoader::FindSubsystem($subsysDirectory, $sub);
        # This will be cleared if we decide to skip the subsystem.
        my $processSub = 1;
        # Are we loading the subsystem and its roles?
        if (! $opt->roles) {
            # The subsystem will not be loaded. Insure the subsystem exists.
            if (! $loader->Check(Subsystem => $sub, $subHash)) {
                print "Subsystem \"$sub\" not found in database-- skipped.\n";
                $stats->Add(subsystemNotFound => 1);
                $processSub = 0;
            }
        } else {
            # Here we are loading the subsystem root and roles. We need to make
            # sure the old version of the subsystem is gone. If we are clearing, it is
            # already gone. If we are in missing-mode, we skip the subsystem if it is
            # already there.
            if (! $opt->clear) {
                my $subFound = $loader->Check(Subsystem => $sub, $subHash);
                if (! $subFound) {
                    # It's a new subsystem, so we have no worries.
                    $stats->Add(subsystemAdded => 1);
                } elsif ($opt->missing) {
                    # It's an existing subsystem, but we are skipping existing subsystems.
                    print "Subsystem \"$sub\" already in database-- skipped.\n";
                    $stats->Add(subsystemSkipped => 1);
                    $processSub = 0;
                } else {
                    # Here we must delete the subsystem.
                    print "Deleting existing copy of $sub.\n";
                    my $delStats = $shrub->Delete(Subsystem => $sub);
                    $stats->Accumulate($delStats);
                    $stats->Add(subsystemReplaced => 1);
                }
            }
            if ($processSub) {
                # Now the old subsystem is gone. We must load the new one. We start with the root
                # record.
                print "Creating $sub.\n";
                $loader->InsertObject('Subsystem', id => $sub, security => $priv, version => 1);
                # Next come the roles of the subsystem. This may cause new roles to be added to
                # the Role table. Get the roles.
                my $roles = $loader->GetNamesFromFile("$subDir/Roles", "role");
                # Loop through the roles. Note we need to track the ordinal position of each role.
                my $ord = 0;
                for my $role (@$roles) {
                    my $roleID = $funcLoader->ProcessRole($role);
                    $loader->InsertObject('Subsystem2Role', 'from-link' => $sub, 'to-link' => $roleID,
                            ordinal => ++$ord);
                    $stats->Add(roleForSubsystem => 1);
                }
            }
        }
        # If we are keeping this subsystem, remember it.
        if ($processSub) {
            $subDirs{$sub} = $subDir;
        }
    }
    # Delete the subsystem array and hash to save memory.
    undef $subs;
    undef $subHash;
    # The next phase is protein processing.
    if ($opt->prots) {
        # Create a hash of the genome directories.
        print "Locating genomes.\n";
        my $genomeHash = $loader->FindGenomeList($genomeDirectory);
        # This is a two-level hash that will map PEGs to funtions, organized by genome.
        my %genomePegs;
        # Loop through the subsystems.
        for my $sub (sort keys %subDirs) {
            # Get the subsystem's directory.
            my $subDir = $subDirs{$sub};
            print "Loading proteins and functions for $sub.\n";
            # Now we want to get the proteins covered by this subsystem and associate functions
            # with them. We go through the list of pegs. Later we will find the protein information
            # in the relevant genome files.
            print "Processing subsystem PEGs.\n";
            my $ih = $loader->OpenFile("$subDir/PegsInSubsys", 'peg');
            while (my $pegDatum = $loader->GetLine($ih, 'peg')) {
                # Get the fields of the peg data line.
                my ($peg, $function) = @$pegDatum;
                # Save the PEG's information.
                my $genome = SeedUtils::genome_of($peg);
                $genomePegs{$genome}{$peg} = $function;
                $stats->Add(subsystemPeg => 1);
            }
        }
        # Loop through the genomes found in the peg list.
        print "Processing genomes for function mapping.\n";
        for my $genome (sort keys %genomePegs) {
            my $gPegHash = $genomePegs{$genome};
            # Do we have this genome?
            my $genomeDir = $genomeHash->{$genome};
            if (! $genomeDir) {
                print "Genome $genome not found in the repository.\n";
                $stats->Add(genomeNotFound => 1);
            } else {
                $funcLoader->ConnectPegFunctions($genome, $genomeDir, $gPegHash,
                        translateLinks => 0, priv => $priv);
            }
        }
        # Finally, we link the subsystems to the genomes already in the database.
        if ($opt->links) {
            # This hash will contain the genomes found in the database.
            my %genomesLoaded = map { $_ => 1 } $shrub->GetFlat('Genome', "", [], 'id');
            print scalar(keys %genomesLoaded) . " genomes already loaded in database.\n";
            # Loop through the subsystems.
            for my $sub (sort keys %subDirs) {
                # Get this subsystem's directory.
                my $subDir = $subDirs{$sub};
                print "Connecting genomes for $sub.\n";
                # Open the genome connection file.
                my $ih = $loader->OpenFile("$subDir/GenomesInSubsys", 'genome');
                # Loop through the genomes.
                while (my $gData = $loader->GetLine($ih, 'genome')) {
                    my ($genome, undef, $varCode) = @$gData;
                    # Is this genome in the database?
                    if ($genomesLoaded{$genome}) {
                        # Yes, connect it.
                        $loader->InsertObject('Subsystem2Genome', 'from-link' => $sub, 'to-link' => $genome,
                                variant => $varCode);
                        $stats->Add(genomeConnected => 1);
                    } else {
                        # No, skip it.
                        $stats->Add(genomeSkipped => 1);
                    }
                }
            }
        }
    }
    # Close and upload the load files.
    print "Unspooling load files.\n";
    $loader->Close();
    # Compute the total time.
    my $timer = time - $startTime;
    $stats->Add(totalTime => $timer);
    my $subCount = scalar(keys %subDirs);
    if ($subCount > 0) {
        my $perSub = ($timer / $subCount);
        print "$perSub seconds per subsystem.\n";
    }
    # Tell the user we're done.
    print "Database processed.\n" . $stats->Show();

