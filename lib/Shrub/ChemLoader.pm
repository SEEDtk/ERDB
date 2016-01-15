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
produces B<Reaction>, B<Complex2Reaction>, and B<Reaction2Compound>.

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
    my $repoDir = "$FIG_Config::data/ModelSEEDDatabase";
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


=head2 Public Methods

=head3 RefreshFiles

    $chemLoader->RefreshFiles();

Insure we have the latest copy of the biochemistry data files.

=cut

sub RefreshFiles {
    my ($self) = @_;
    # Save the current directory.
    my $saveDir = cwd();
    # Insure the directory exists.
    my $repoDir = $self->{repoDir};
    if (! -d "$repoDir") {
        # Directory not found, clone it.
        print "Creating ModelSEED repo.\n";
        chdir $FIG_Config::data;
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
        my ($rid) = $roleMgr->Processs($role);
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

1;