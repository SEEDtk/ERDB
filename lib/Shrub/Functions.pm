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


package Shrub::Functions;

    use strict;
    use warnings;

=head1 SHrub Function Manager

This object manages the insertion of functions into the database. Like roles,
functions are a resource, and we have to deal with the possibility that the function
we want may or may not already exist. The tricky part about functions is that
we need to connect them to their constituent roles. This object has both shared-mode
and exclusive-mode subclasses.

There are two special types of functions that do not have consistuent roles. The
function C<hypothetical protein>, which is sometimes indicated by an empty string,
is not considered a real function. There is a similar set of functions representing
features with unknown purpose other than the protein-generating pegs. Finally, there
are functions transcribed from external sources that appear damaged and are not
parsed for roles. Both types of functions are temporary placeholders, and the hope
is they will all eventually be replaced by real functional descriptions that accurately
reflect the roles.

A function may have multiple roles. Three I<separator characters> are used to split
the roles:

=over 4

=item semi-colon (C<;>)

The feature has one of several roles, but the precise role is unknown.

=item at-sign (<@>)

The feature has multiple roles, all effected by the same protein domain.

=item slash (C</>)

The feature hash multiple roles, each effected by a different protein domain.

=back

The roles are ordered for the slash, and unordered for the other separator. Thus

    Urease beta subunit / Urease gamma subunit
    Urease gamma subunit / Urease beta subunit

are different functions, but

    Urease beta subunit @ Urease gamma subunit
    Urease gamma subunit @ Urease beta subunit

are the same.

This object contains the following fields.

=over 4

=item loader

L<Shrub::DBLoader> object for inserting into the database.

=item stats

L<Stats> object for tracking statistics about our operations.

=item roles

L<Shrub::Roles> object for inserting roles.

=back

=head2 Special Methods

=head3 new

    my $varname = Shrub::Functions->new($loader, %options);

Create a new, blank function manager.

=over 4

=item loader

L<Shrub::DBLoader> object for inserting into the database.

=item options

Hash of options, containing zero or more of the following keys.

=over 8

=item exclusive

If TRUE, it will be presumed we have exclusive access to the database, and significant optimization is
possible. The default is FALSE.

=item roles

A L<Shrub::Roles> object for managing and inserting roles. If none is provided, this object will
create one.

=cut

sub new {
    # Get the parameters.
    my ($class, $loader, %options) = @_;
    # Get the role manager. If the client didn't give us one, we create it.
    my $roles = $options{roles} // Shrub::Roles->new($loader, %options);
    # Create the object.
    my $retVal = { loader => $loader, roles => $roles };
    # Are we exclusive?
    if ($options{exclusive}) {
        # Construct us exclusively.
        require Shrub::Functions::Exclusive;
        Shrub::Functions::Exclusive::init($retVal, %options);
    } else {
        # Construct us for a shared database.
        require Shrub::Functions::Shared;
        Shrub::Functions::Shared::init($retVal, %options);
    }
    # Return the newly-created object.
    return $retVal;
}

=head2 Subclass Methods

=head3 db

    my $shrub = $funMgr->db;

Return the attached L<Shrub> object.

=cut

sub db {
    return $_[0]->{loader}->db;
}

=head3 stats

    my $stats = $funMgr->stats;

Return the attached statistics object.

=cut

sub stats {
    return $_[0]->{loader}->stats;
}

=head2 Query Methods

=head3 Parse

    my ($statement, $sep, \%roles, $comment) = $funMgr->Parse($function);

or

    my ($statement, $sep, \%roles, $comment) = Shrub::Functions::Parse($function);

Parse a functional assignment. This method breaks it into its constituent roles,
pulls out the comment and the separator character, and computes the checksum.

=over 4

=item function

Functional assignment to parse.

=item RETURN

Returns a five-element list containing the following.

=over 8

=item statement

The text of the function with the EC numbers and comments removed.

=item sep

The separator character. For a single-role function, this is always C<@>. For multi-role
functions, it could also be C</> or C<;>.

=item roles

Reference to a hash mapping each constituent role to its checksum.

=item comment

The comment string containing in the function. If there is no comment, will be an empty
string.

=back

=back

=cut

sub Parse {
    # Convert from the instance form of the call to a direct call.
    shift if UNIVERSAL::isa($_[0], __PACKAGE__);
    # Get the parameters.
    my ($function) = @_;
    # Separate out the comment (if any). Note we convert an undefined function
    # to an empty string.
    my $statement = $function // "";
    my $comment = "";
    if ($function && $function =~ /(.+?)\s*[#!](.+)/) {
        ($statement, $comment) = ($1, $2);
    }
    # The roles and the separator will go in here.
    my @roles;
    my $sep = ' ';
    # This will be the role hash.
    my %roles;
    # Check for suspicious elements.
    my $malformed;
    if (! $statement || $statement eq 'hypothetical protein') {
        # Here we have a hypothetical protein. This is considered well-formed but without
        # any roles.
    } elsif ($function =~ /\b(?:similarit|blast\b|fasta|identity)|%|E=/i) {
        # Here we have suspicious elements.
        $malformed = 1;
    } else {
        # Parse out the roles.
        my @roleParts = split(/\s*(\s\@|\s\/|;)\s+/, $statement);
        # Check for a role that is too long.
        if (grep { length($_) > 250 } @roles) {
            $malformed = 1;
        } elsif (scalar(@roleParts) == 1) {
            # Here we have the normal case, a single-role function.
            @roles = @roleParts;
        } else {
            # With multiple roles, we need to extract the separator and peel out the
            # roles. Note we insure that all of the roles have text; this is to
            # correct a common error in function definitions.
            $sep = substr($roleParts[1], -1);
            for (my $i = 0; $i < scalar(@roleParts); $i += 2) {
                my $rolePart = $roleParts[$i];
                if ($rolePart =~ /\w/i) {
                    push @roles, $rolePart;
                }
            }
        }
    }
    # If we are not malformed, we must separate out the roles.
    if (! $malformed) {
        # Here we have to compute a checksum from the roles and the separator.
        my @normalRoles = map { Shrub::Roles::Normalize($_) } @roles;
        # Now create the role hash.
        for (my $i = 0; $i < scalar(@roles); $i++) {
            $roles{$roles[$i]} = Checksum($normalRoles[$i]);
        }
    }
    # Return the parsed function data.
    return ($statement, $sep, \%roles, $comment);
}



=head2 Public Manipulation Methods

=head3 Process

    my $funcID = $funcMgr->Process($statement, $sep, $roleH);

Store a function in the database and return its ID. If the function
already exists, there will be no update, the ID will simply be returned.
Note that the parameters are basically the same as the output from
L</Parse>.

=over 4

=item statement

Function description text.

=item sep

Separator character for the roles (or a space, if there is only one role).

=item roleH

Reference to a hash mapping each role's text to its checksum.

=item RETURN

Returns the ID of the function.

=back

=cut

sub Process {
    # Get the parameters.
    my ($self, $statement, $sep, $roleH) = @_;
    # Get the loader object.
    my $loader = $self->{loader};
    # Get the statistics object.
    my $stats = $loader->stats;
    # Is this function already in the database?
    ## TODO check function.
    my $retVal;
    if (! $retVal) {
        # No, we must insert it. Start by insuring we have its roles.
        my @roleIDs;
        for my $role (keys %$roleH) {
            # Get this role's checksum.
            my $roleCheck = $roleH->{$role};
            # Get the role's ID.
            my ($roleID) = $self->ProcessRole($role, $roleCheck);
            push @roleIDs, $roleID;
        }
        ## TODO remove checksum from function.
        ## TODO function key formed from role IDs. How to know if new function created? if not, no F2R insert
        ## TODO $funThing->Insert(sep => $sep, description => $statement);
        # Connect the roles to the function.
        for my $roleID (@roleIDs) {
            $loader->InsertObject('Function2Role', 'from-link' => $retVal, 'to-link' => $roleID);
            $stats->Add(function2role => 1);
        }
    }
    # Return the function ID.
    return $retVal;
}


=head2 Virtual Methods

=cut

## TODO virtual methods for Shrub::Functions


1;