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


package Shrub::Taxonomy;

    use strict;
    use warnings;

=head1 Shrub Taxonomy Tree

This package creates a giant data structure that describes the taxonomy tree. It places each genome in its
taxonomy group, and relates each taxonomy group to its children. Finally, for each taxonomy group it tracks
the total number of genomes in it.

The object contains the following fields.

=over 4

=item taxGenomes

Reference to a hash that maps each leaf taxonomic group to a list of its genomes. Each genome is stored as 
an [id, name] 2-tuple.

=item taxNames

Reference to a hash that maps each taxonomy ID to a name.

=item taxNameIdx

Reference to a hash that maps each taxonomy name to a taxonomy ID.

=item taxChildren

Reference to a hash that maps each taxonomy ID to a list of the IDs of its child groups.

=item taxCounts

Reference to a hash that maps each taxonomy ID to the number of genomes its contains.

=item domains

Reference to a list of the top-level domain groups.

=back

=head2 Special Methods

=head3 new

    my $taxTree = Shrub::Taxonomy->new($shrub);

Create a Shrub taxonomy tree.

=over 4

=item shrub

L<Shrub> object for accessing the database.

=back

=cut

sub new {
    my ($class, $shrub) = @_;
    # We must create our in-memory taxonomy tree. This hash maps names to taxonomy IDs.
    my %taxNameIdx;
    # This hash maps IDs to names.
    my %taxNames;
    # This hash maps each taxonomy ID to the number of genomes underneath it.
    my %taxCounts;
    # This hash maps each taxonomy ID to a list of its children.
    my %taxChildren;
    # This hash maps each leaf taxonomy ID to a list of its genome specifiers.
    my %taxGenomes;
    # This is a list of the fields we need from each taxonomy node.
    my $taxFields = 'TaxonomicGrouping(id) TaxonomicGrouping(scientific-name) TaxonomicGrouping(domain)';
    # This is a list of taxonomic groups still to examine.
    my @groups;
    # This is a list of the top-level groups.
    my @domains;
    # Get all the genomes and their taxonomic IDs.
    my @genomes = $shrub->GetAll('Genome TaxonomicGrouping', '', [], "Genome(id) Genome(name) $taxFields");
    # Process these genomes.
    for my $genomeData (@genomes) {
        my ($genomeID, $genomeName, $taxID, $taxName, $domain) = @$genomeData;
        $taxCounts{$taxID}++;
        if (! $taxNames{$taxID}) {
            $taxNameIdx{$taxName} = $taxID;
            $taxNames{$taxID} = $taxName;
            if (! $domain) {
                push @groups, $taxID;
            } else {
                push @domains, $taxID;
            }
        }
        push @{$taxGenomes{$taxID}}, [$genomeID, $genomeName];
    }
    # Now we move up the tree, connecting children to parents.
    while (@groups) {
        my $group = pop @groups;
        # Get the parent group.
        my ($parent) = $shrub->GetAll('IsInTaxonomicGroup TaxonomicGrouping', 'IsInTaxonomicGroup(from-link) = ?',
                [$group], $taxFields);
        my ($taxID, $taxName, $domain) = @$parent;
        if (! $taxNames{$taxID}) {
            $taxNameIdx{$taxName} = $taxID;
            $taxNames{$taxID} = $taxName;
            if (! $domain) {
                push @groups, $taxID;
            } else {
                push @domains, $taxID;
            }
        }
        push @{$taxChildren{$taxID}}, $group;
    }
    # Create the object from all the hashes just built.
    my $retVal = {
        taxCounts => \%taxCounts,
        taxNames => \%taxNames,
        taxChildren => \%taxChildren,
        domains => \@domains,
        taxGenomes => \%taxGenomes,
        taxNameIdx => \%taxNameIdx,
    };
    bless $retVal, $class;
    # We are not done yet. The counts are not filled in. Recursively count the
    # genomes in each domain.
    for my $group (@domains) {
        $retVal->countChildren($group, \%taxChildren, \%taxCounts);
    }
    # Return this object.
    return $retVal;
}

=head2 Query Methods

=head3 children

    my $childList = $taxTree->children($groupID);

Return a list of a taxonomic grouping's children.

=over 4

=item groupID

ID of the relevant taxonomic group.

=item RETURN

Returns a reference to a list of the IDs for the taxonomic group's children.

=back

=cut

sub children {
    my ($self, $groupID) = @_;
    # Get the children hash.
    my $taxChildren = $self->{taxChildren};
    # Get the children of the incoming group,
    my $retVal = $taxChildren->{$groupID} // [];
    return $retVal;
}

=head3 count

    my $count = $taxTree->count($groupID);

Return the number of genomes underneath a specified taxonomic grouping.

=over 4

=item groupID

ID of the relevant taxonomic grouping.

=item RETURN

Returns the number of genomes under the grouping.

=back

=cut

sub count {
    my ($self, $groupID) = @_;
    # Get the count hash.
    my $taxCounts = $self->{taxCounts};
    # Get the count of the incoming group.
    my $retVal = $taxCounts->{$groupID} // 0;
    return $retVal;
}


=head3 name

    my $name = $taxTree->name($groupID);

Return the name of a specified taxonomic grouping.

=over 4

=item groupID

ID of the relevant taxonomic grouping.

=item RETURN

Returns the name of the grouping.

=back

=cut

sub name {
    my ($self, $groupID) = @_;
    # Get the name hash.
    my $taxNames = $self->{taxNames};
    # Get the name of the incoming group.
    my $retVal = $taxNames->{$groupID} // "unknown taxa $groupID";
    return $retVal;
}


=head3 genomes

    my $genomeList = $taxTree->genomes($groupID);

Get the list of genomes in the specified taxonomic grouping. If the grouping is not a leaf, the
genomes of all its children will be combined.

=over 4

=item groupID

ID of the relevant taxonomic grouping.

=item RETURN

Returns a reference to a list of genome specifiers, each a 2-tuple containing (0) a genome ID and (1) a
genome name.

=back

=cut

sub genomes {
    my ($self, $groupID) = @_;
    # Get the traversal hashes.
    my $taxGenomes = $self->{taxGenomes};
    my $taxChildren = $self->{taxChildren};
    # This is the request stack for the taxonomic groups.
    my @requests = ($groupID);
    # This is the return list.
    my @retVal;
    # Process the stack.
    while (@requests) {
        my $request = pop @requests;
        if ($taxGenomes->{$request}) {
            # Here we have a leaf.
            push @retVal, @{$taxGenomes->{$request}};
        } else {
            # Here we have children.
            push @requests, @{$taxChildren->{$request}};
        }
    }
    # Return the full list.
    return \@retVal;
}

=head3 tax_id

    my $groupID = $taxTree->tax_id($name);

Return the ID of the taxonomic grouping with the specified scientific name. Note that aliases are not currently
supported.

=over 4

=item name

Name of the taxonomic grouping whose ID is desired.

=item RETURN

Returns the grouping ID of the named group, or C<undef> if the name is not found.

=back

=cut

sub tax_id {
    my ($self, $name) = @_;
    my $taxNameIdx = $self->{taxNameIdx};
    return $taxNameIdx->{$name};
}

=item domains

    my $domainH = $taxTree->domains;

Return a reference to a hash mapping all the high-level domain IDs to their names.

=cut

sub domains {
    my ($self) = @_;
    my $taxNames = $self->{taxNames};
    my %retVal = map { $_ => $taxNames->{$_} } @{$self->{domains}};
    return \%retVal;
}

=head2 Internal Methods

=head3 countChildren

    my $count = $taxTree->countChildren($groupID);

Recursively count the genomes found under the specified taxonomic group. This method also updates the
taxCounts hash.

=over 4

=item groupID

ID of the group whose genome count is desired.

=item RETURN

Returns the number of genomes under the group.

=back

=cut
 
sub countChildren {
    my ($self, $groupID) = @_;
    # Get the working hashes.
    my $taxCounts = $self->{taxCounts};
    my $taxChildren = $self->{taxChildren};
    # This will be the return value.
    my $retVal = 0;
    # Get all the child groups.
    my @children = @{$taxChildren->{$groupID}};
    # Loop through them.
    for my $child (@children) {
        # Do we already know the count?
        if (defined $taxCounts->{$child}) {
            # Yes, use it.
            $retVal += $taxCounts->{$child};
        } else {
            # No, compute it.
            $retVal += $self->countChildren($child);
        }
    }
    # Save our result for future queries.
    $taxCounts->{$groupID} = $retVal;
    # Return the count.
    return $retVal;
}


1;