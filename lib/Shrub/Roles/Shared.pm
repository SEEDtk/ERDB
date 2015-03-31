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


package Shrub::Roles::Shared;

    use strict;
    use warnings;
    use ERDBtk::ID::Magic;
    use base qw(Shrub::Roles);

=head1 Shrub Role Manager for Shared Database Access

This object manages insertion of roles into the database. The process is complicated because a new
role may have an EC number that needs to be stored in the existing roles. This object assumes we
don't have control of the database and everything needs to be done the hard way.

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
    # Create the inserter. Note again that we handle the check field here.
    $self->{inserter} = ERDBtk::ID::Magic->new(Role => $loader, $loader->stats,
            nameField => 'description');
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
    # Get the statistics object and the database object.
    my $stats = $self->stats;
    my $shrub = $self->db;
    # These variables will hold the proposed ID components.
    my ($prefix, $suffix);
    # The role ID will be stored in here.
    my $retVal;
    # Loop until were are done.
    while (! $retVal) {
        # Does the role exist in the database?
        my ($roleData) = $shrub->GetAll('Role', 'Role(checksum) = ?', [$checkSum], 'id ec-number tc-number');
        if ($roleData) {
            # Yes. Get the role's data.
            $stats->Add(roleFound => 1);
            my ($id, $oldEC, $oldTC) = @$roleData;
            # Verify the EC and TC numbers. If one of these updates fails, it means someone else
            # stored a number, so we're ok.
            $self->CheckRoleNumber('ec-number', $oldEC, $ecNum, $id);
            $self->CheckRoleNumber('tc-number', $oldTC, $tcNum, $id);
            # Denote we have the role.
            $retVal = $id;
        } else {
            # The role does not exist. We try to insert it. Compute a prefix and suffix.
            # If we already have them, we just increment the suffix.
            if ($suffix) {
                $suffix++;
            } else {
                ($prefix, $suffix) = $self->{inserter}->ComputeID($roleText);
            }
            # Attempt to insert the role.
            my $okFlag = $shrub->InsertObject('Role', { id => "$prefix$suffix",
                    checksum => $checkSum, 'ec-number' => $ecNum, 'tc-number' => $tcNum,
                    hypo => $hypo, description => $roleText }, dup => 'ignore');
            if ($okFlag) {
                # The insert worked. We are done.
                $retVal = $prefix . $suffix;
                $stats->Add(roleInserted => 1);
            } else {
                #The insert failed. We must try again.
                $stats->Add(roleInsertFailed => 1);
            }
        }
    }
    # Return the ID found.
    return $retVal;
}

=head2 Private Utilities

=head3 CheckRoleNumber

    my $okFlag = $roleMgr->CheckRoleNumber($field, $oldValue, $newValue, $id);

Merge a new value into a role field. This is usually used for the EC and TC numbers. If the old
value is empty and the new value is not, an optimistic update will be attempted. The method will
return FALSE if the update fails.

=over 4

=item field

Name of the field in question.

=item oldValue

Current value of the field in the database record.

=item newValue

Proposed new value of the field.

=item id

ID of the role in question.

=item RETURN

Returns TRUE if no update is needed or a successful udpate was made, else FALSE. If FALSE, the role
needs to be re-read from the database. It could be updated or it could be deleted.

=back

=cut

sub CheckRoleNumber {
    # Get the parameters.
    my ($self, $field, $oldValue, $newValue, $id) = @_;
    # Get the database and the statistics object.
    my $shrub = $self->db;
    my $stats = $self->stats;
    # Denote we are successful.
    my $retVal = 1;
    # Do we need to update the field?
    if ($newValue && ! $oldValue) {
        # Yes. Try an optimistic update. The update will fail if the role changes from what we
        # thought it was.
        my $count = $shrub->UpdateField("Role($field)", $oldValue, $newValue, 'Role(id) = ?', [$id]);
        if ($count) {
            # The update worked.
            $stats->Add("role-" . $field . "Updated" => 1);
        } else {
            # The update failed.
            $stats->Add("role-" . $field . "UpdateFailed" => 1);
            $retVal = 0;
        }
    }
    # Return the success indicator.
    return $retVal;
}

1;
