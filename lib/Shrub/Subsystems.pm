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


package Shrub::Subsystems;

    use strict;
    use warnings;
    use Shrub;
    use ServicesUtils;
    
=head1 Find Subsystems for a Genome

This package is used to perform a basic role-based projection of subsystems onto genomes. The
core method takes as input a L<Shrub> object and a table of feature IDs to function IDs. The
basic algorithm is then as follows:

=over 4

=item 1

Use the functions to compute the subsystems for each feature.

=item 2

For each subsystem found, match the set of roles therein to known variants. If all of the roles in a
variant are present, then it is a I<candidate variant> for the subsystem.

=item 3

Choose the candidate variant with the most roles and output the features in the subsystem based on
that variant.

=back

=head2 Public Methods

=head3 Project

    my $subsystemHash = Shrub::Subsystems::Project($shrub, \%featureAssignments);

Compute the subsystems that occur for a specified set of annotated
features.

=over 4

=item shrub

A L<Shrub> object for accessing the database.

=item featureAssignments

Reference to a hash mapping each feature ID to its functional assignment. The functions should be in the
form of function IDs (which in turn are role IDs with separators).

=item RETURN

Returns a reference to a hash mapping each subsystem ID to a 2-tuple containing (0) a variant code, and (1) a
reference to a list of [role, fid] tuples.

=back

=cut

sub Project {
    # Get the parameters.
    my ($shrub, $featureAssignments) = @_;
    # Declare the return variable.
    my %retVal;
    # Create a hash of all the roles in the genome.
    my %roles;
    for my $fid (keys %$featureAssignments) {
        my $function = $featureAssignments->{$fid};
        my @roles = Shrub::roles_of_func($function);
        for my $role (@roles) {
            push @{$roles{$role}}, $fid;
        }
    }
    # We need all the subsystems containing these roles. We will search in batches of 100.
    my %subs;
    my @roles = keys %roles;
    for (my $i = 0; $i < @roles; $i += 100) {
        my $i1 = $i + 99;
        if ($i >= $#roles) { $i = $#roles };
        my @parms = @roles[$i .. $i1];
        my $filter = 'Role2Subsystem(from-link) IN (' . join(', ', map { '?' } @parms) . ')';
        my @subData = $shrub->GetAll('Role2Subsystem',  $filter, \@parms, 'from-link to-link');
        for my $subDatum (@subData) {
            my ($role, $sub) = @$subDatum;
            $subs{$sub}{$role} = 1;
        }
    }
    # Now process each of the subsystems. For each subsystem we have a sub-hash of all its roles
    # currently in the genome. We want the best match, that is, the variant whose roles are fully
    # represented and has the most roles in it. 
    # Search for the best match.
    for my $sub (sort keys %subs) {
        # These variables will contain the best match so far.
        my ($bestVariant, $bestRoles);
        # This is the role count for the best match.
        my $bestCount = 0;
        # Get the hash of represented roles in this subsystem.
        my $subRolesH = $subs{$sub};
        my $represented = scalar keys %$subRolesH;
        # Get all the maps for this subsystem.
        my @maps = $shrub->GetAll('Subsystem2Map VariantMap', 'Subsystem2Map(from-link) = ?',
                [$sub], 'VariantMap(variant-code) VariantMap(map) VariantMap(size)');
        # Loop through the maps, searching for a good one.
        for my $map (@maps) {
            my ($variant, $roles, $count) = @$map;
            my @roles = split ' ', $roles;
            # Do we have enough represented roles to fill this variant?
            if ($count <= $represented) {
                # Yes. Count the roles found.
                my $found = scalar(grep { $subRolesH->{$_} } @roles);
                if ($found == $count) {
                    # Here all the roles in the map are represented in the genome.
                    if ($count > $bestCount) {
                        # Here this match is the best one found so far.
                        ($bestVariant, $bestRoles, $bestCount) = ($variant, \@roles, $count);
                    }
                }
            }
        }
        # Did we find a match?
        if ($bestCount) {
            # Yes. Create the variant description.
            my @variantRoles;
            for my $role (@$bestRoles) {
                my $rolePegs = $roles{$role};
                for my $peg (@$rolePegs) {
                    push @variantRoles, [$role, $peg];
                }
            }
            $retVal{$sub} = [$bestVariant, \@variantRoles];
        }
    }
    # Return the result.
    return \%retVal;
}


=head3 ProjectForGto

    my $subsystemHash = Shrub::Subsystems::ProjectForGto($shrub, $gto, %options);

Compute the subsystems that occur in a genome defined by a L<GenomeTypeObject>. This method essentially
computes the feature assignment hash and then calls L</Project>.

=over 4

=item shrub

L<Shrub> object for accessing the database.

=item gto

L<GenomeTypeObject> for the genome on which the subsystems should be projected.

=item options

A hash of options, including zero or more of the following.

=over 8

=item store

If TRUE, the subsystems will be stored directory into the GenomeTypeObject. The default is FALSE.

=item RETURN

Returns a reference to a hash mapping each subsystem ID to a 2-tuple containing (0) a variant code, and (1) a
reference to a list of [role, fid] tuples.

=back

=cut

sub ProjectForGto {
    my ($shrub, $gto, %options) = @_;
    # Get the feature list.
    my $featureList = ServicesUtils::json_field($gto, 'features');
    # Loop through the features, creating the assignment hash.
    my %assigns;
    for my $featureData (@$featureList) {
        my $fid = $featureData->{id};
        my $funcID = $shrub->desc_to_function($featureData->{function});
        if ($funcID) {
            $assigns{$fid} = $funcID;
        }
    }
    # Project the subsystems.
    my $retVal = Project($shrub, \%assigns);
    # Store the results if needed.
    if ($options{store}) {
        my %subs;
        for my $sub (keys %$retVal) {
            my $projectionData = $retVal->{$sub};
            my ($variant, $subRow) = @$projectionData;
            my %cells;
            for my $subCell (@$subRow) {
                my ($role, $fid) = @$subCell;
                push @{$cells{$role}}, $fid; 
            }
            $subs{$sub} = [$variant, \%cells];
        }
        $gto->{subsystems} = \%subs;
    }
    return $retVal;
}

=head3 ProjectForGenome

    my $subsystemHash = Shrub::Subsystems::ProjectForGenome($shrub, $genomeID, $priv);

Compute the subsystems that occur in a genome in the database. This method essentially
computes the feature assignment hash and then calls L</Project>.

=over 4

=item shrub

L<Shrub> object for accessing the database.

=item genomeID

ID of the genome onto which to project the subsystem.

=item priv

Privilege level for the assignments to use in the projection.

=item RETURN

Returns a reference to a hash mapping each subsystem ID to a 2-tuple containing (0) a variant code, and (1) a
reference to a list of [role, fid] tuples.

=back

=cut

sub ProjectForGenome {
    my ($shrub, $genomeID, $priv) = @_;
    # Get the feature assignments.
    my %assigns = map { $_->[0] => $_->[1] } $shrub->GetAll('Genome2Feature Feature2Function',
            'Genome2Feature(from-link) = ? AND Feature2Function(security) = ?', [$genomeID, $priv],
            'Feature2Function(from-link) Feature2Function(to-link)');
    # Project the subsystems.
    my $retVal = Project($shrub, \%assigns);
    return $retVal;
}


1;