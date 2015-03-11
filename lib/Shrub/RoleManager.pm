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


package Shrub::RoleManager;

    use strict;
    use warnings;
    use ERDB::ID::Magic;
    use Tracer;

=head1 Shrub Role Manager

This object manages insertion of roles into the database. The process is complicated because a new
role may have an EC number that needs to be stored in the existing roles. The object has both
exclusive and shared operating modes.

This object has the following fields.

=over 4

=item erdb

The L<Shrub> object for accessing the database itself.

=item stats

A L<Stats> object for tracking statistics in the current session.

=item inserter

An L<ERDB::ID::Magic> object for inserting roles.

=back

=head2 Special Methods

=head3 new

    my $roleMgr = Shrub::RoleManager->new($loader, $stats, %options);

Create a new role management object. The parameters are as follows.

=over 4

=item loader

A I<loader object> that is to be used for inserting new records. This can either by an actual
L<Shrub> object or a <Shrub::DBLoader> object.

=item stats

A L<Stats> object for tracking statistics about the current session.

=item options

A hash of options for this role manager, including zero or more of the following keywords.

=over 8

=item exclusive

If TRUE, then it is assumed we have exclusive access to the database and significant optimization
is possible. If FALSE, then the operations will be designed to allow concurrent update. The default
is FALSE.

=back

=back

=cut

sub new {
    # Get the parameters.
    my ($class, $loader, $stats, %options) = @_;
    # Create the inserter.
    my $inserter = ERDB::ID::Magic('Role', $loader, $stats, checkField => 'checksum', nameField => 'description',
            exclusive => $options{exclusive});
    # This will be the return value.
    my $retVal = { erdb => $loader->db, stats => $stats, inserter => $inserter };
    # Are we exclusive?
    if ($options{exclusive}) {
        # Yes. Construct us as exclusive.
        require Shrub::RoleManager::Exclusive;
        Shrub::RoleManager::Exclusive::init($retVal, %options);
    } else {
        # No. Construct us as shared.
        require Shrub::RoleManager::Shared;
        Shrub::RoleManager::Shared::init($retVal, %options);
    }
    # Return the object.
    return $retVal;
}

=head2 Subclass Methods

=head3 db

    my $shrub = $roleMgr->db;

Return the attached L<Shrub> object.

=cut

sub db {
    return $_[0]->{erdb};
}

=head3 stats

    my $stats = $roleMgr->stats;

Return the attached statistics object.

=cut

sub stats {
    return $_[0]->{stats};
}

=head2 Public Manipulation Methods

=head3 ProcessRole

    my ($roleID, $roleMD5) = $funcLoader->ProcessRole($role, $checksum);

Return the ID of a role in the database. If the role does not exist, it will be inserted.

=over 4

=item role

Text of the role to find.

=item checksum (optional)

If the checksum of the role is already known, it can be passed in here.

=item RETURN

Returns a two-element list containing (0) the ID of the role in the database and (1) the
role's MD5 checksum.

=back

=cut

sub ProcessRole {
    # Get the parameters.
    my ($self, $role) = @_;
    # Get the role inserter.
    my $roleThing = $self->{inserter};
    # Parse the role components.
    my ($roleText, $ecNum, $tcNum, $hypo) = Shrub::ParseRole($role);
    # Compute the checksum.
    my $roleNorm = Shrub::RoleNormalize($role);
    my $checkSum = Shrub::Checksum($roleNorm);
    # Insert the role.
    my $retVal = $self->InsertRole($checkSum, $ecNum, $tcNum, $hypo, $roleText);
    # Return the role information.
    return ($retVal, $checkSum);
}


=head2 Virtual Methods

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
    Confess("Pure virtual Shrub::RoleMgr::InsertRole called.");
}


=head3 Close

    $roleMgr->Close();

Emit all the queued updates to the Role table.

=cut

sub Close {
    # The default is to do nothing.
}

1;
