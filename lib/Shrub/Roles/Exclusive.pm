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


package Shrub::Roles::Exclusive;

    use strict;
    use warnings;
    use ERDBtk::ID::Magic::Exclusive;
    use ERDBtk::ID::Magic;
    use base qw(Shrub::Roles);

=head1 Shrub Role Manager

This object manages insertion of roles into the database. The process is complicated because a new
role may have an EC number that needs to be stored in the existing roles. The object has both
exclusive and shared operating modes.

In addition to the fields in the base class object, this object has the following fields.

=over 4

=item loader

A I<loader object> that is to be used for inserting new records. This can either by an actual
L<Shrub> object or a <Shrub::DBLoader> object.

=item roleNums

Reference to a hash that maps each role ID to a 2-tuple containing (0) its current EC number and
(1) its current TC number.

=item updates

Reference to a hash containing all the roles that need to be updated or inserted during the L</Close>
operation. The hash maps each role ID to a field hash suitable for L<ERDBtk/Insert>.

=item checkHash

Reference to a hash mapping the checksums of the existing roles to their IDs.

=item inDB

A hash of the roles currently in the database.

=back

=head2 Special Methods

=head3 init

    Shrub::Roles::Exclusive::init($roleMgr, $loader, %options);

Initialize and bless a role management object for environments with
exclusive control of the database. The inserter must be created and the
various optimization hashes loaded.

=over 4

=item roleMgr

Partially-constructed Shrub::Roles object.

=item loader

A I<loader object> that is to be used for inserting new records. This can either by an actual
L<Shrub> object or a <Shrub::DBLoader> object.

=item options

A hash of options for the object, including zero or more of the following.

=over 8

=item roleFile

Name of a tab-delimited file containing role ID information from a previous database so that
IDs remain constant. Each row of the file should contain (0) a role ID, (1) the corresponding
checksum, (2) an optional ec number, and (3) an optional tc number.

=back

=back

=cut

sub init {
    # Get the parameters.
    my ($self, $loader, %options) = @_;
    # We need to fill in these hashes from the database and the role file.
    my (%roleNums, %checkHash, %prefixHash, %inDB);
    # Start by checking the caller-provided file.
    if ($options{roleFile}) {
        open(my $ih, '<', $options{roleFile}) || die "Could not open role file: $!";
        while (! eof $ih) {
            my $line = <$ih>;
            chomp $line;
            my ($roleID, $checksum, $ecNum, $tcNum) = split /\t/, $line;
            # Save the role ID for this checksum.
            $checkHash{$checksum} = $roleID;
            # Denote we do not know the EC and TC numbers for this role.
            $roleNums{$roleID} = ['', ''];
            # Compute the next available suffix.
            ERDBtk::ID::Magic::Exclusive::UpdatePrefixHash(\%prefixHash, $roleID);
        }
    }
    # This query will override the above with what's already in the database.
    my $q = $self->{erdb}->Get('Role', '', [], 'id checksum ec-number tc-number');
    while (my $roleData = $q->Fetch()) {
        my ($roleID, $checksum, $ecNum, $tcNum) = $roleData->Values('id checksum ec-number tc-number');
        # Save the role ID for this checksum.
        $checkHash{$checksum} = $roleID;
        # Save the EC and TC numbers for this role.
        $roleNums{$roleID} = [$ecNum, $tcNum];
        # Denote this role is in the database.
        $inDB{$roleID} = 1;
        # Compute the next available suffix.
        ERDBtk::ID::Magic::Exclusive::UpdatePrefixHash(\%prefixHash, $roleID);
    }
    # Store the hashes we just computed.
    $self->{roleNums} = \%roleNums;
    $self->{checkHash} = \%checkHash;
    $self->{inDB} = \%inDB;
    # Create the Magic Name ID inserter. Note that we don't provide the check field. We do the
    # checksum handling in this object.
    $self->{inserter} = ERDBtk::ID::Magic->new(Role => $loader, $loader->stats, exclusive => 1,
            nameField => 'description', hashes => [\%prefixHash]);
    # Create the update hash. It is initially empty.
    $self->{updates} = {};
    # Remember the loder.
    $self->{loader} = $loader;
    # Insure we are queued to close when the loader closes.
    $loader->QueueSubObject($self);
    # Bless this object.
    bless $self, __PACKAGE__;
}


=head2 Virtual Overrides

=head3 InsertRole

    my $roleID = $roleMgr->InsertRole($checkSum, $ecNum, $tcNum, $hypo, $roleText);

Insure a role is in the database. If the role already exists, its current ID will be returned.
If it exists but does not have the correct EC or TC number, these will be updated. If it does
not exist at all, it will be created.

=over 4

=item checkSum

MD5 checksum of the normalized role text.

=item ecNum

EC number, or an empty string if there is none.

=item tcNum

TC number, or an empty string if there is none.

=item hypo

TRUE if the role is hypothetical, else FALSE

=item roleText

Final role description.

=item RETURN

Returns the ID of the role in the database.

=back

=cut

sub InsertRole {
    # Get the parameters.
    my ($self, $checkSum, $ecNum, $tcNum, $hypo, $roleText) = @_;
    # Get the checksum hash, the number hash, the in-db hash, and the update hash.
    my $checkHash = $self->{checkHash};
    my $roleNums = $self->{roleNums};
    my $inDB = $self->{inDB};
    my $updates = $self->{updates};
    # Get the statistics object.
    my $stats = $self->stats;
    # We'll set this to TRUE if we need to queue a role update.
    my $needUpdate;
    # Does this role already exist?
    my $retVal = $checkHash->{$checkSum};
    if ($retVal) {
        # It does.
        $stats->Add(roleFound => 1);
        # Do the EC and TC numbers match?
        my $savedInfo = $roleNums->{$retVal};
        my ($oldEC, $oldTC) = @$savedInfo;
        my $bestEC = ($oldEC || $ecNum);
        my $bestTC = ($oldTC || $tcNum);
        if ($bestEC ne $oldEC || $bestTC ne $oldTC) {
            # No. Prepare for an update.
            $needUpdate = 1;
            $ecNum = $bestEC;
            $tcNum = $bestTC;
            $stats->Add(roleNumsUpdate => 1);
        }
        # Is it in the database?
        if (! $inDB->{$retVal}) {
            # No, we need an update.
            $needUpdate = 1;
            $stats->Add(roleRecovered => 1);
        }
    } else {
        # The role does not exist, we need to insert it.
        $stats->Add(roleNotFound => 1);
        # Compute its ID.
        $retVal = $self->{inserter}->ComputeID($roleText);
        # Save its information in the checksum hash.
        $checkHash->{$checkSum} = $retVal;
        # Denote we need an update.
        $needUpdate = 1;
    }
    # Do we need an update?
    if ($needUpdate) {
        # Yes. Queue the update for when we close.
        $stats->Add(roleUpdateQueued => 1);
        $updates->{$retVal} = { id => $retVal, checksum => $checkSum, description => $roleText,
                'ec-number' => $ecNum, 'tc-number' => $tcNum, hypo => $hypo };
        # Update the role number hash.
        $roleNums->{$retVal} = [$ecNum, $tcNum];
        # Denote that now it is technically in the database.
        $inDB->{$retVal} = 1;
    }
    # Return the role ID.
    return $retVal;
}


=head3 Close

    $roleMgr->Close();

Emit all the queued updates to the Role table.

=cut

sub Close {
    # Get the parameters.
    my ($self) = @_;
    # Get the statistics object.
    my $stats = $self->stats;
    # Get the loader object. Denote that Roles are in replace mode.
    my $loader = $self->{loader};
    $loader->ReplaceMode('Role');
    # Loop through the updates.
    my $updates = $self->{updates};
    for my $roleID (keys %$updates) {
        $stats->Add(roleUpdateUnspooled => 1);
        $loader->InsertObject('Role', %{$updates->{$roleID}});
    }
    # Denote the updates are no longer queued.
    $self->{updates} = {};
}


1;
