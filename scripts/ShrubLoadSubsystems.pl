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
directory. If omitted, it will be computed from information in the L<FIG_Config> module.

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

=item all

Implies C<links>, C<pegs>, and C<roles>.

=item genomeDir

Directory containing the genome source files. If not specified, the default will be
computed from information in the L<FIG_Config> file.

=back

=cut

    # Start timing.
    my $startTime = time;
    $| = 1; # Prevent buffering on STDOUT.
    # Parse the command line.
    my $opt = ScriptUtils::Opts('subsysDirectory',
            ["slow|s", "use individual inserts rather than table loads"],
            ["subsystems=s", "name of a file containing a list of the subsystems to use"],
            ["missing|m", "only load subsystems not already in the database"],
            ["clear|c", "clear the subsystem tables before loading", { implies => 'missing' }],
            ["genomeDir|g=s", "genome directory containing the data to load", { default => "$FIG_Config::shrub_dir/Inputs/GenomeData" }]
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
    # Create the function loader utility object.
    my $funcLoader;
    print "Initializing function and role tables.\n";
    $funcLoader = ShrubFunctionLoader->new($loader, rolesOnly => 1, slow => $opt->slow);
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
        $subs = $loader->GetNamesFromFile(subsystem => $opt->subsystems);
        print scalar(@$subs) . " subsystems read from " . $opt->subsystems . "\n";
    } else {
        # Here we are processing all the subsystems in the subsystem directory.
        $subs = [ map { Shrub::NormalizedName($_) } grep { -d "$subsysDirectory/$_" } $loader->OpenDir($subsysDirectory, 1) ];
        print scalar(@$subs) . " subsystems found in repository at $subsysDirectory.\n";
    }
    # Get the list of tables.
    my @tables = qw(Subsystem2Row SubsystemRow Row2Genome Row2Cell SubsystemCell Role2Cell Subsystem2Role Feature2Cell);
    # Are we clearing?
    if ($opt->clear) {
        # Yes. Erase all the subsystem tables.
        print "CLEAR option specified.\n";
        $loader->Clear('Subsystem', @tables);
    }
    # Set up the tables we are going to load. Old subsystem data will be deleted before the new data is
    # loaded. We only set up the tables this way in non-slow mode. In slow mode, our failure to do so will
    # cause the loader package to do manual inserts.
    if (! $opt->slow) {
        $loader->Open(@tables);
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
            # Now the old subsystem is gone. We must load the new one. We start with the root
            # record.
            print "Creating $sub.\n";
            # We need the metadata.
            my $metaHash = $loader->ReadMetaData("$subDir/Info", required => [qw(privileged row-privilege)]);
            # Default the version to 1.
            my $version = $metaHash->{version} // 1;
            # Insert the subsystem record. If we already have an ID, it will be reused. Otherwise a magic name will
            # be created.
            $subID = $shrub->CreateMagicEntity(Subsystem => 'name', id => $subID, name => $sub, privileged => $metaHash->{privileged},
                    version => $version);
            # Next come the roles. This will map role abbreviations to a 2-tuple consisting of (0) the role ID
            # and (1) the column number.
            my %roleMap;
            # Open the role input file.
            my $rh = $loader->OpenFile(role => "$subDir/Roles");
            # Loop through the roles. Note we need to track the ordinal position of each role.
            my $ord = 0;
            while (my $roleData = $loader->GetLine(role => $rh)) {
                # Get this role's data.
                my ($abbr, $role) = @$roleData;
                # Compute the role ID. If the role is new, this inserts it in the database.
                my ($roleID) = $funcLoader->ProcessRole($role);
                # Link the subsystem to the role.
                $loader->InsertObject('Subsystem2Role', 'from-link' => $sub, 'to-link' => $roleID,
                        ordinal => $ord, abbr => $abbr);
                $stats->Add(roleForSubsystem => 1);
                # Save the role's abbreviation and ID.
                $roleMap{$abbr} = [$roleID, $ord];
                # Increment the column number.
                $ord++;
            }
            # Now we create the rows.
            print "Connecting genomes for $sub.\n";
            # Get the row privilege.
            my $rowPrivilege = $metaHash->{'row-privilege'};
            # This hash will map row numbers to lists of cell IDs.
            my %rowMap;
            # Open the genome connection file.
            my $ih = $loader->OpenFile(genome => "$subDir/GenomesInSubsys");
            # Loop through the genomes.
            while (my $gData = $loader->GetLine(genome => $ih)) {
                my ($genome, undef, $varCode, $row) = @$gData;
                # Normalize the variant code.
                my $needsCuration = 0;
                if ($varCode =~ /^\*(.+)/) {
                    $needsCuration = 1;
                    $varCode = $1;
                }
                # Is this genome in the database?
                if ($loader->CheckCached(Genome => $genome, \%genomes)) {
                    # No, skip it.
                    $stats->Add(genomeSkipped => 1);
                } else {
                    # Yes, create a row for it.
                    my $rowID = $shrub->NewID();
                    $loader->InsertObject('Subsystem2Row', 'from-link' => $subID, 'to-link' => $rowID);
                    $loader->InsertObject('SubsystemRow', id => $rowID, 'needs-curation' => $needsCuration,
                            privilege => $rowPrivilege, 'variant-code' => $varCode);
                    $loader->InsertObject('Genome2Row', 'from-link' => $rowID, 'to-link' => $genome);
                    $stats->Add(genomeConnected => 1);
                    # Now build the cells.
                    my %cellMap;
                    for my $abbr (keys %roleMap) {
                        # Get this role's data.
                        my ($ord, $roleID) = @{$roleMap{$abbr}};
                        # Compute the cell ID.
                        my $cellID = $shrub->NewID();
                        # Create the subsystem cell.
                        $loader->InsertObject('Row2Cell', 'from-link' => $rowID, 'ordinal' => $ord,
                                'to-link' => $cellID);
                        $loader->InsertObject('SubsystemCell', id => $cellID);
                        $loader->InsertObject('Role2Cell', 'from-link' => $roleID, 'to-link' => $cellID);
                        # Put it in the map.
                        $cellMap{$abbr} = $cellID;
                    }
                    # Remember the row's cells.
                    $rowMap{$row} = \%cellMap;
                }
            }
            # Close the genome file.
            close $ih;
            # Now we link the pegs to the subsystem cells.
            print "Processing subsystem PEGs.\n";
            # Open the PEG data file.
            $ih = $loader->OpenFile(peg => "$subDir/PegsInSubsys");
            while (my $pegDatum = $loader->GetLine(peg => $ih)) {
                # Get the fields of the peg data line.
                my ($peg, $abbr, undef, $row) = @$pegDatum;
                # Do we care about this genome? Note that if the genome was found, it is guaranteed to
                # be in our cache (%genomes) by now. We checked it when we built the rows.
                my $genome = SeedUtils::genome_of($peg);
                if (! $genomes{$genome}) {
                    $stats->Add(subsysPegSkipped => 1);
                } else {
                    # We want this peg. Put it in the cell.
                    my $cellID = $rowMap{$row}{$abbr};
                    $loader->InsertObject('Feature2Cell', 'from-link' => $peg, 'to-link' => $cellID);
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
    # Tell the user we're done.
    print "Database processed.\n" . $stats->Show();

