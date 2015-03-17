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


package Shrub::Roles;

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

=head2 IMPORTANT NOTE

This object assumes that roles won't be deleted by other processes. To insure the methods
work, you cannot delete roles unless you have exclusive database access.

=head2 Special Methods

=head3 new

    my $roleMgr = Shrub::Roles->new($loader, %options);

Create a new role management object. The parameters are as follows.

=over 4

=item loader

A L<Shrub::DBLoader> object for inserting into the database.

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
    my ($class, $loader, %options) = @_;
    # This will be the return value.
    my $retVal = { erdb => $loader->db, stats => $loader->stats, inserter => undef };
    # Are we exclusive?
    if ($options{exclusive}) {
        # Yes. Construct us as exclusive.
        require Shrub::Roles::Exclusive;
        Shrub::Roles::Exclusive::init($retVal, $loader, %options);
    } else {
        # No. Construct us as shared.
        require Shrub::Roles::Shared;
        Shrub::Roles::Shared::init($retVal, $loader, %options);
    }
    # Return the object.
    return $retVal;
}

=head2 Role Text Analysis Methods

=head3 EC_PATTERN

    $string =~ /$Shrub::EC_PATTERN/;

Pre-compiled pattern for matching EC numbers.

=cut

    our $EC_PATTERN = qr/\(\s*E\.?C\.?(?:\s+|:)(\d\.(?:\d+|-)\.(?:\d+|-)\.(?:n?\d+|-)\s*)\)/;

=head3 TC_PATTERN

    $string =~ /$Shrub::TC_PATTERN/;

Pre-compiled pattern for matchin TC numbers.

=cut

    our $TC_PATTERN = qr/\(\s*T\.?C\.?(?:\s+|:)(\d\.[A-Z]\.(?:\d+|-)\.(?:\d+|-)\.(?:\d+|-)\s*)\)/;

=head3 Parse

    my ($roleText, $ecNum, $tcNum, $hypo) = $roleMgr->Parse($role);

or

    my ($roleText, $ecNum, $tcNum, $hypo) = Shrub::Roles::Parse($role);

Parse a role. The EC and TC numbers are extracted and an attempt is made to determine if the role is
hypothetical.

=over 4

=item role

Text of the role to parse.

=item RETURN

Returns a four-element list consisting of the main role text, the EC number (if any),
the TC number (if any), and a flag that is TRUE if the role is hypothetical and FALSE
otherwise.

=back

=cut

sub Parse {
    # Convert from the instance form of the call to a direct call.
    shift if UNIVERSAL::isa($_[0], __PACKAGE__);
    # Get the parameters.
    my ($role) = @_;
    # Extract the EC number.
    my ($ecNum, $tcNum) = ("", "");
    my $roleText = $role;
    if ($role =~ /(.+?)\s*$EC_PATTERN\s*(.*)/) {
        $roleText = $1 . $3;
        $ecNum = $2;
    } elsif ($role =~ /(.+?)\s*$TC_PATTERN\s*(.*)/) {
        $roleText = $1 . $3;
        $tcNum = $2;
    }
    # Fix spelling problems.
    $roleText = FixupRole($roleText);
    # Check for a hypothetical.
    my $hypo = SeedUtils::hypo($roleText);
    # If this is a hypothetical with a number, change it.
    if ($roleText eq 'hypothetical protein' || ! $roleText) {
        if ($ecNum) {
            $roleText = "putative protein $ecNum";
        } elsif ($tcNum) {
            $roleText = "putative transporter $tcNum";
        }
    }
    # Return the parse results.
    return ($roleText, $ecNum, $tcNum, $hypo);
}

=head3 Normalize

    my $normalRole = Shrub::Roles::Normalize($role);

or

    my $normalRole = $roleMgr->Normalize($role);

Normalize the text of a role by removing extra spaces and converting it to lower case.

=over 4

=item role

Role text to normalize. This should be taken from the output of L</Parse>.

=item RETURN

Returns a normalized form of the role.

=back

=cut

sub Normalize {
    # Convert from the instance form of the call to a direct call.
    shift if UNIVERSAL::isa($_[0], __PACKAGE__);
    # Get the parameters.
    my ($role) = @_;
    # Remove the extra spaces and punctuation.
    $role =~ s/[\s,.:]{2,}/ /g;
    # Translate unusual white characters.
    $role =~ s/\s/ /;
    # Convert to lower case.
    my $retVal = lc $role;
    # Return the result.
    return $retVal;
}


=head3 FixupRole

    my $roleText = Shrub::Roles::FixupRole($role);

Perform basic fixups on the text of a role. This method is intended for internal use, and it performs
spelling-type normalizations required both when computing a role's checksum or formatting the role
for storage.

=over 4

=item role

The text of a role.

=item RETURN

Returns the fixed-up text of a role.

=back

=cut

sub FixupRole {
    my ($retVal) = @_;
    # Fix spelling mistakes.
    $retVal =~ s/^\d{7}[a-z]\d{2}rik\b|\b(?:hyphothetical|hyothetical)\b/hypothetical/ig;
    # Trim spaces;
    $retVal =~ s/^\s+//;
    $retVal =~ s/\s+$//;
    # Remove quoting.
    $retVal =~ s/^"//;
    $retVal =~ s/"$//;
    # Fix extra spaces.
    $retVal =~ s/\s+/ /g;
    # Return the fixed-up role.
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

=head3 Process

    my ($roleID, $roleMD5) = $roleMgr->Process($role, $checksum);

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

sub Process {
    # Get the parameters.
    my ($self, $role, $checkSum) = @_;
    # Get the role inserter.
    my $roleThing = $self->{inserter};
    # Parse the role components.
    my ($roleText, $ecNum, $tcNum, $hypo) = Parse($role);
    # Compute the checksum.
    if (! defined $checkSum) {
        my $roleNorm = Normalize($roleText);
        $checkSum = Shrub::Checksum($roleNorm);
    }
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
    Confess("Pure virtual Shrub::Roles::InsertRole called.");
}


=head3 Close

    $roleMgr->Close();

Emit all the queued updates to the Role table.

=cut

sub Close {
    # The default is to do nothing.
}

1;
