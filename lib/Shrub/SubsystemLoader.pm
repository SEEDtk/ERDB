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


package Shrub::SubsystemLoader;

    use strict;
    use warnings;
    use Shrub::Roles;
    use Shrub::DBLoader;
    use Digest::MD5;
    use ERDBtk::ID::Magic;

=head1 Shrub Subsystem Load Helper

This object provides utilities for loading subsystems from an L<ExchangeFormat>
subsystem repository. It contains the following fields.

=over 4

=item loader

A L<Shrub::DBLoader> object for manipulating the database and the repository.

=item roleMrg

A L<Shrub::Roles> object for computing role IDs.

=item inserter

An L<ERDBtk::ID> object for inserting subsystem records.

=back

=cut

    # This is a list of the tables we are loading.
    use constant LOAD_TABLES => qw(Subsystem Role SubsystemRow SubsystemCell Subsystem2Role Feature2Cell);

=head2 Special Methods

=head3 new

    my $subLoader = Shrub::SubsystemLoader->new($loader, %options);

Return a new, blank subsystem load helper.

=over 4

=item loader

A L<Shrub::DBLoader> object for manipulating the database and the input repository.

=item options

A hash containing zero or more of the following options.

=over 8

=item roleMrg

A L<Shrub::Roles> object for computing role IDs. If none is provided, an
object will be created internally.

=item exclusive

TRUE if we have exclusive access to the database, else FALSE. The default is FALSE.

=item slow

TRUE if we are to load using individual inserts, FALSE if we are to load by spooling
inserts into files for mass loading.

=back

=back

=cut

sub new {
    # Get the parameters.
    my ($class, $loader, %options) = @_;
    # Get the slow-load flag.
    my $slow = $options{slow} || 0;
    # Get the role-loader object.
    my $roleMrg = $options{roleMrg};
    # If the role loader was not provided, create one.
    if (! $roleMrg) {
        $roleMrg = Shrub::Roles->new($loader, exclusive => $options{exclusive});
    }
    # If we are NOT in slow mode, prepare the tables for loading.
    if (! $slow) {
        $loader->Open(LOAD_TABLES);
    }
    # Create the subsystem inserter.
    my $inserter = ERDBtk::ID::Magic->new(Subsystem => $loader, $loader->stats, exclusive => $options{exclusive},
            checkField => 'checksum', nameField => 'name');
    # Create the object.
    my $retVal = {
        loader => $loader,
        roleMrg => $roleMrg,
        inserter => $inserter
    };
    # Bless and return it.
    bless $retVal, $class;
    return $retVal;
}

=head2 Public Manipulation Methods

=head3 Clear

    $subLoader->Clear();

Recreate the subsystem-related tables.

=cut

sub Clear {
    # Get the parameters.
    my ($self) = @_;
    # Get the loader object.
    my $loader = $self->{loader};
    # CLear the tables.
    $loader->Clear('Subsystem', LOAD_TABLES);
}

=head3 SelectSubsystems

    my $subList = $loader->SelectSubsystems($subsystemSpec, $subsysDirectory);

Determine the subsystems to load.

=over 4

=item subsystemSpec

Either C<all> to load all the subsystems in the repository, or the name of a tab-delimited file
containing subsystem names in the first column.

=item subsysDirectory

The name of the directory containing the subsystem repository.

=item RETURN

Returns a reference to a list of the names of the subsystems to load.

=back

=cut

sub SelectSubsystems {
    my ($self, $subsystemSpec, $subsysDirectory) = @_;
    my $loader = $self->{loader};
    my $retVal;
    if ($subsystemSpec ne 'all') {
        # Here we have a subsystem list.
        $retVal = $loader->GetNamesFromFile(subsystem => $subsystemSpec);
        print scalar(@$retVal) . " subsystems read from $subsystemSpec.\n";
    } else {
        # Here we are processing all the subsystems in the subsystem directory.
        $retVal = [ map { Shrub::NormalizedName($_) }
                grep { -d "$subsysDirectory/$_" } $loader->OpenDir($subsysDirectory, 1) ];
        print scalar(@$retVal) . " subsystems found in repository at $subsysDirectory.\n";
    }
    # Return the subsystem list.
    return $retVal;
}


=head3 LoadSubsystem

    my $actualID = $subLoader->LoadSubsystem($subID => $sub, $subDir, \%genomes);

Load a subsystem into the database. The subsystem cannot already exist: if it did exist,
all traces of it must have been erased by a L<ERDBtk/Delete> call.

=over 4

=item subID

The proposed subsystem ID, or C<undef> if the subsystem ID should be computed. Use
an explicit ID when replacing a deleted subsystem and C<undef> when creating a new
one.

=item sub

Name of the subsystem.

=item subDir

Name of the directory containing the subsystem source files in L<ExchangeFormat>.

=item genomes (optional)

If specified, a reference to a hash containing genome IDs as keys. The genome ID will
be mapped to C<1> if it is in the database and C<0> if it is known to not be in the
database. This is used to improve performance and may be omitted freely.

=item RETURN

Returns the ID of the loaded subsystem.

=back

=cut

sub LoadSubsystem {
    # Get the parameters.
    my ($self, $subID => $sub, $subDir, $genomeHash) = @_;
    # Get the loader object and the database.
    my $loader = $self->{loader};
    my $shrub = $loader->db;
    my $stats = $loader->stats;
    # Insure we have a genome cache. If the client didn't give us one,
    # use an empty hash.
    $genomeHash //= {};
    # Get the function loader.
    my $roleMrg = $self->{roleMrg};
    # Load the subsystem.
    print "Creating $sub.\n";
    # We need the metadata.
    my $metaHash = $loader->ReadMetaData("$subDir/Info", required => [qw(privileged row-privilege)]);
    # Default the version to 1.
    my $version = $metaHash->{version} // 1;
    # Compute the checksum.
    my $checksum = Digest::MD5::md5_base64($sub);
    # Insert the subsystem record. If we already have an ID, it will be reused. Otherwise a magic name will
    # be created.
    my $retVal = $self->{inserter}->Insert(id => $subID, name => $sub, privileged => $metaHash->{privileged},
            version => $version, checksum => $checksum);
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
        my ($roleID) = $roleMrg->Process($role);
        # Link the subsystem to the role.
        $loader->InsertObject('Subsystem2Role', 'from-link' => $retVal, 'to-link' => $roleID,
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
        if (! $loader->CheckCached(Genome => $genome, $genomeHash)) {
            # No, skip it.
            $stats->Add(subsystemGenomeSkipped => 1);
        } elsif ($varCode =~ /^-?\d+$/ && $varCode <= 0) {
            # Yes, but it's an incomplete variant.
            $stats->Add(subsystemGenomeVacant => 1);
        } else {
            # Yes, and it's ok, so create a row for it.
            my $rowID = "$retVal:$row";
            $loader->InsertObject('SubsystemRow', id => $rowID, 'needs-curation' => $needsCuration,
                    privilege => $rowPrivilege, 'variant-code' => $varCode, Subsystem2Row_link => $retVal, Genome2Row_link => $genome);
            $stats->Add(genomeConnected => 1);
            # Now build the cells.
            my %cellMap;
            for my $abbr (keys %roleMap) {
                # Get this role's data.
                my ($roleID, $ord) = @{$roleMap{$abbr}};
                # Compute the cell ID.
                my $cellID = "$rowID:$abbr";
                # Create the subsystem cell.
                $loader->InsertObject('SubsystemCell', id => $cellID, Row2Cell_link => $rowID, Row2Cell_ordinal => $ord, Role2Cell_link => $roleID);
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
        if (! $genomeHash->{$genome}) {
            $stats->Add(subsysPegSkipped => 1);
        } else {
            # Get the row's cells.
            my $cellMap = $rowMap{$row};
            # Only proceed if we are keeping this row.
            if (! $cellMap) {
                $stats->Add(pegInVacantRowSkipped => 1);
            } else {
                # We want this peg. Put it in the cell.
                my $cellID = $cellMap->{$abbr};
                $loader->InsertObject('Feature2Cell', 'from-link' => $peg, 'to-link' => $cellID);
            }
        }
    }
    # Return the new subsystem's ID.
    return $retVal;
}


1;
