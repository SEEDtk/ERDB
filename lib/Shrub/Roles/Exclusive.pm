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
    use ERDB::ID::Magic::Exclusive;
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
operation. The hash maps each role ID to a field hash suitable for L<ERDB/Insert>.

=item checkHash

Reference to a hash mapping the checksums of the existing roles to their IDs.

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

A hash of options for the object. Currently there are none of relevance to this
subclass.

=back

=cut

sub init {
    # Get the parameters.
    my ($self, $loader, %options) = @_;
    # We need to fill in these hashes from the database.
    my (%roleNums, %checkHash, %prefixHash);
    my $q = $self->{erdb}->Get('Role', '', [], 'id checksum ec-number tc-number');
    while (my $roleData = $q->Fetch()) {
        my ($roleID, $checksum, $ecNum, $tcNum) = $roleData->Values('id checksum ec-number tc-number');
        # Save the role ID for this checksum.
        $checkHash{$checksum} = $roleID;
        # Save the EC and TC numbers for this role.
        $roleNums{$roleID} = [$ecNum, $tcNum];
        # Compute the next available suffix.
        ERDB::ID::Magic::Exclusive::UpdatePrefixHash($roleID);
    }
    # Store the hashes we just computed.
    $self->{roleNums} = \%roleNums;
    $self->{checkHash} = \%checkHash;
    # Create the Magic Name ID inserter. Note that we don't provide the check field. We do the
    # checksum handling in this object.
    $self->{inserter} = ERDB::ID::Magic(Role => $loader, $loader->stats, exclusive => 1,
            nameField => 'description', hashes => [\%prefixHash]);
    # Create the update hash. It is initially empty.
    $self->{updates} = {};
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
    # Get the checksum hash, the number hash, and the update hash.
    my $checkHash = $self->{checkHash};
    my $roleNums = $self->{roleNums};
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
        my $savedInfo = @{$roleNums->{$retVal}};
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
    # Get the loader object. Denote that we are in replace mode.
    my $loader = $self->{loader};
    $loader->ReplaceMode('Role');
    # Loop through the updates.
    my $updates = $self->{updates};
    for my $roleID (keys %$updates) {
        $stats->Add(roleUpdateUnspooled => 1);
        $loader->InsertObject('Role', %{$updates->{$roleID}});
    }
}


1;
