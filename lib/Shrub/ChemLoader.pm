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


package Shrub::ChemLoader;

    use strict;
    use warnings;
    use Shrub::Roles;
    use File::Copy::Recursive;
    use Cwd;

=head1 Shrub Biochemistry Load Utilities

This package contains utilities for loading biochemistry data. The master copy of the data is in a series
of GitHub repos. The tables to load are

=over 4

=item 1

Complex

=item 2

Reaction

=item 3

Compound

=item 4

Pathway

=item 5

Role2Complex

=item 6

Complex2Reaction

=item 7

Pathway2Reaction

=item 8

Reaction2Compound

=back

This information is taken from the following tab-delimited files.

=over 4

=item Complexes.tsv

This file comes from L<https://github.com/ModelSEED/ModelSEEDDatabase/Templates> and produces
B<Complex> and B<Role2Complex>.

=item Roles.tsv

This file comes from L<https://github.com/ModelSEED/ModelSEEDDatabase/Templates> and maps the
role IDs in other files to Shrub role descriptions.

=item compounds.master.tsv

This file comes from L<https://github.com/ModelSEED/ModelSEEDDatabase/Biochemistry> and
produces B<Compound>.

=item reactions.master.tsv

This file comes from L<https://github.com/ModelSEED/ModelSEEDDatabase/Biochemistry> and
produces B<Reaction> and B<Reaction2Compound>.

=item Reactions.tsv

This file comes from L<https://github.com/ModelSEED/ModelSEEDDatabase/Templates/Microbial> and
produces B<Complex2Reaction>.

=item plantdefault.pathways.tsv

This file comes from L<https://github.com/ModelSEED/ModelSEEDDatabase/Pathways> and produces
B<Pathway2Reaction> and B<Pathway>.

=back

This object contains the following fields.

=over 4

=item loader

L<Shrub::DBLoader> object for accessing the database and statistics.

=item repoDir

Name of the directory containing the source repo files.

=item roleMgr

L<Shrub::Roles> object for processing roles.

=item roleMap

Reference to a hash that maps ModelSEED role IDs to Shrub role IDs.

=item slow

TRUE if we are to load using individual inserts, FALSE if we are to load by spooling
inserts into files for mass loading.

=back

=cut

    # This is the list of tables we are loading.
    use constant LOAD_TABLES => qw(Complex Reaction Compound Pathway Role2Complex Complex2Reaction
            Pathway2Reaction Reaction2Compound);

=head2 Special Methods

=head3 new

    my $chemLoader = ChemLoader->new($loader, %options);

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

=item roleMgr

A L<Shrub::Roles> object for computing role IDs. If none is provided, one will be created internally.

=back

=back

=cut

sub new {
    # Get the parameters.
    my ($class, $loader, %options) = @_;
    # Get the slow-load flag.
    my $slow = $options{slow} || 0;
    # Get the function-loader object.
    my $roleMgr = $options{roleMgr};
    # Compute the name of the biochem repo directory.
    my $repoDir = "$FIG_Config::data/Inputs/ModelSEEDDatabase";
    # If the role loader was not provided, create one.
    if (! $roleMgr) {
        $roleMgr = Shrub::Roles->new($loader, exclusive => $options{exclusive});
    }
    # If we are NOT in slow-loading mode, prepare the tables for spooling.
    if (! $slow) {
        $loader->Open(LOAD_TABLES);
    }
    # Create the object.
    my $retVal = { loader => $loader, roleMgr => $roleMgr, slow => $slow,
            repoDir => $repoDir, roleMap => {} };
    # Bless and return the object.
    bless $retVal, $class;
    return $retVal;
}


=head3 RefreshFiles

    Shrub::ChemLoader::RefreshFiles();

Insure we have the latest copy of the biochemistry data files.

=cut

sub RefreshFiles {
    # Save the current directory.
    my $saveDir = cwd();
    # Insure the directory exists.
    my $repoDir = "$FIG_Config::data/Inputs/ModelSEEDDatabase";
    if (! -d "$repoDir") {
        # Directory not found, clone it.
        print "Creating ModelSEED repo.\n";
        chdir "$FIG_Config::data/Inputs";
        my @output = `git clone https://github.com/ModelSEED/ModelSEEDDatabase`;
        if (grep { $_ =~ /fatal:\s+(.+)/ } @output) {
            die "Error retrieving ModelSEEDDatabase: $1";
        }
    } else {
        # Directory found, refresh it.
        print "Pulling ModeSEED repo.\n";
        chdir $repoDir;
        my @output = `git pull`;
        if (grep { $_ =~ /conflict/ } @output) {
            die "ModelSEEDDatabase pull failed.";
        }
    }
    # Restore the directory.
    chdir $saveDir;
}


=head2 Public Methods

=head3 Process

    $chemLoader->Process();

Load the biochemistry data from the master data files.

=cut

sub Process {
    my ($self) = @_;
    # Open the Roles file and read the role mapping.
    $self->LoadRoleMap();
    # Load the complexes and connect them to the roles.
    $self->LoadComplexes();
    # Load the compounds.
    $self->LoadCompounds();
    # Load the reactions and connect them to the compounds and complexes.
    $self->LoadReactions();
}

=head2 Internal Methods

=head3 LoadRoleMap

    $chemLoader->LoadRoleMap();

Create a map from ModelSEED role IDs to Shrub Role IDs.

=cut

sub LoadRoleMap {
    my ($self) = @_;
    # Get the statistics object.
    print "Loading ModelSEED role map.\n";
    my $stats = $self->{loader}->stats;
    # Declare the role hash.
    my %roles;
    # Get the role manager.
    my $roleMgr = $self->{roleMgr};
    # Open the roles file.
    my $roleFile = "$self->{repoDir}/Templates/Roles.tsv";
    open(my $ih, "<$roleFile") || die "Could not open ModelSEED role file: $!";
    # Discard the label line.
    my $line = <$ih>;
    # Loop through the data lines.
    while (! eof $ih) {
        $line = <$ih>;
        my ($mid, $role) = split /\t/, $line;
        $stats->Add(modelSeedRoleIn => 1);
        my ($rid) = $roleMgr->Process($role);
        $roles{$mid} = $rid;
    }
    # Save the role map.
    $self->{roleMap} = \%roles;
}


=head3 LoadComplexes

    $chemLoader->LoadComplexes();

Load the complex table and the complex-to-role relationship.

=cut

sub LoadComplexes {
    my ($self) = @_;
    # Get the loader and statistics objects.
    print "Loading ModelSEED complexes.\n";
    my $loader = $self->{loader};
    my $stats = $loader->stats;
    # Get the role map.
    my $roleH = $self->{roleMap};
    # Open the complexes file.
    my $cpxFile = "$self->{repoDir}/Templates/Complexes.tsv";
    open(my $ih, "<$cpxFile") || die "Could not open ModelSEED complex file: $!";
    # Discard the label line.
    my $line = <$ih>;
    # Loop through the data lines.
    while (! eof $ih) {
        $line = <$ih>;
        $stats->Add(complexLineIn => 1);
        if ($line =~ /^(\S+)\t.+\t(.+)$/) {
            my ($cpxID, $roles) = ($1, $2);
            $stats->Add(complexLineParsed => 1);
            $loader->InsertObject('Complex', id => $cpxID);
            # Split the roles.
            if ($roles eq 'null') {
                $stats->Add(complexLineNull => 1);
            } else {
                my @roles = split /\|/, $roles;
                for my $role (@roles) {
                    # We get a role ID, a triggering indicator, and unknown junk.
                    my ($rid, $trigger) = split /;/, $role;
                    if (! $roleH->{$rid}) {
                        print "Role $rid not found for complex $cpxID.\n";
                        $stats->Add(modelSEEDroleNotFound => 1);
                    } else {
                        my $triggerFlag = ($trigger eq 'triggering' ? 1 : 0);
                        $loader->InsertObject('Role2Complex', 'from-link' => $roleH->{$rid},
                                'to-link' => $cpxID, triggering => $triggerFlag);
                    }
                }
            }
        } else {
            print "Invalid complex line: $line";
            $stats->Add(badComplexLine => 1);
        }
    }
}

=head3 LoadCompounds

    $chemLoader->LoadCompounds();

Load the compound table.

=cut

sub LoadCompounds {
    my ($self) = @_;
    # Get the loader and statistics objects.
    print "Loading ModelSEED compounds.\n";
    my $loader = $self->{loader};
    my $stats = $loader->stats;
    # Open the compounds file.
    my $cpdFile = "$self->{repoDir}/Biochemistry/compounds.master.tsv";
    open(my $ih, "<$cpdFile") || die "Could not open ModelSEED compounds file: $!";
    # Discard the label line.
    my $line = <$ih>;
    # Loop through the data lines.
    while (! eof $ih) {
        $line = <$ih>;
        $stats->Add(compoundsLineIn => 1);
        chomp $line;
        my ($id, undef, $name, $formula, undef, undef, undef, undef, undef, undef, undef, $cofactor) =
                split /\t/, $line;
        $loader->InsertObject('Compound', id => $id, label => $name, formula => $formula,
                cofactor => $cofactor);
    }
}

=head3 LoadReactions

    $chemLoader->LoadReactions();

Load the reactions table and connect it to the compounds, pathways, and complexes.

=cut

sub LoadReactions {
    my ($self) = @_;
    # Get the loader and statistics objects.
    print "Loading ModelSEED reactions.\n";
    my $loader = $self->{loader};
    my $stats = $loader->stats;
    # Open the reactions file.
    my $reactFile = "$self->{repoDir}/Biochemistry/reactions.master.tsv";
    open(my $ih, "<$reactFile") || die "Could not open ModelSEED reactions file: $!";
    # Discard the label line.
    my $line = <$ih>;
    # We will track reaction IDs in here.
    my %reactions;
    # Loop through the data lines.
    while (! eof $ih) {
        $line = <$ih>;
        $stats->Add(reactionLineIn => 1);
        chomp $line;
        my ($rid, undef, $name, undef, $stoich, undef, undef, undef, undef, $direction) = split /\t/, $line;
        # Save the reaction ID.
        $reactions{$rid} = 1;
        # Create the reaction record.
        $loader->InsertObject('Reaction', id => $rid, name => $name, direction => $direction);
        # Loop through the stoichiometry.
        my @stoichs = split /;/, $stoich;
        for my $stoichData (@stoichs) {
            $stats->Add(reactionStoichIn => 1);
            # We have here a compound and a number. The number is the stoichiometry value,
            # and it is negative for a substrate.
            my ($number, $compound) = split /:/, $stoichData;
            my $product = 1;
            if ($number < 0) {
                $number = -$number;
                $product = 0;
            }
            $loader->InsertObject('Reaction2Compound', 'from-link' => $rid, 'to-link' => $compound,
                    product => $product, stoichiometry => $number);
        }

    }
    # Now we need to link these reactions to complexes.
    close $ih; undef $ih;
    print "Connecting reactions to complexes.\n";
    open($ih, "<$self->{repoDir}/Templates/Microbial/Reactions.tsv") || die "Could not open Reactions.tsv: $!";
    # Discard the label line.
    $line = <$ih>;
    # Loop through the reactions.
    while (! eof $ih) {
        my $line = <$ih>;
        $stats->Add(reactionComplexLineIn => 1);
        if ($line =~ /^(\S+)\t.+\t(cpx\S+)$/) {
            # Here we have a reaction ID and a list of complexes.
            my ($rid, $cpxList) = ($1, $2);
            if (! $reactions{$rid}) {
                $stats->Add(complexReactionNotFound => 1);
            } else {
                $stats->Add(reactionComplexGoodLine => 1);
                my @cpxs = split /\|/, $cpxList;
                for my $cpx (@cpxs) {
                    $stats->Add(reactionComplexItem => 1);
                    $loader->InsertObject('Complex2Reaction', 'from-link' => $cpx, 'to-link' => $rid);
                }
            }
        } else {
            $stats->Add(reactionComplexNullLine => 1);
        }
    }
    # Finally, we connect reactions to pathways.
    close $ih; undef $ih;
    print "Connecting reactions to patheways.\n";
    open($ih, "<$self->{repoDir}/Pathways/plantdefault.pathways.tsv") || die "Could not open ModelSEED pathways file: $!";
    # This hash tracks the pathway names.
    my %pathways;
    # Get the types from the label line.
    $line = <$ih>;
    chomp $line;
    my (undef, @types) = split /\t/, $line;
    # Loop through the reactions.
    while (! eof $ih) {
        my $line = <$ih>;
        $stats->Add(reactionPathwayLineIn => 1);
        my ($rid, @pathLists) = split /\t/, $line;
        if (! $reactions{$rid}) {
            $stats->Add(pathwayReactionNotFound => 1);
        } else {
            # Loop through the type and pathway lists in parallel.
            for (my $i = 0; $i < scalar(@types); $i++) {
                # Get the type and the path list.
                my $type = $types[$i];
                my $pathList = $pathLists[$i];
                # Only proceed for a real pathway list.
                if (! $pathList || $pathList eq 'null') {
                    $stats->Add(nullPathwayList => 1);
                } else {
                    # Get the pathway names.
                    my @paths = split /\|/, $pathList;
                    # Loop through the paths.
                    for my $path (@paths) {
                        # See if we need to create this pathway.
                        if ($pathways{$path}) {
                            # Already present.
                            $stats->Add(oldPathwayFound => 1)
                        } else {
                            # Must create it.
                            $stats->Add(newPathwayFound => 1);
                            $loader->InsertObject('Pathway', id => $path, type => $type);
                            $pathways{$path} = 1;
                        }
                        # Now connect it to the reaction.
                        $loader->InsertObject('Pathway2Reaction', 'from-link' => $path, 'to-link' => $rid);
                    }
                }
            }
        }
    }
}


1;