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


package ERDB::ID::Counter;

    use strict;
    use warnings;
    use base qw(ERDB::ID);

=head1 ERDB AutoCounter Helper

This is the ID helper class for auto-counter IDs. It is a subclass of L<ERDB::ID>, and is used
to insert instances of entities whose ID is generated from a counter value. Each entity type has
a record in the ID table which contains the next available ID. The constructor for this object
allocates a group of ID numbers to this client, which are then consumed when new objects are
created.

In addition to the fields in the base class, this object contains the following fields.

=over 4

=item nextID

Next available entity ID.

=item lastID

Last available entity ID.

=item allocation

Number of IDs to allocate if we run out.

=back

=head2 Special Methods

=head3 DEFAULT_ALLOCATION

This is a constant for the default number of IDs to allocate.

=cut

use constant DEFAULT_ALLOCATION => 100;

=head3 new

    my $helper = ERDB::ID::Counter->new($entityName, $loader, $stats, %options);

Construct a new ID helper object for the specified entity.

=over 4

=item entityName

Name of the entity type that this helper generates IDs for.

=item loader

A loader object used to insert records and access the database.

=item stats

A L<Stats> object used for tracking statistics.

=item options

A hash of options, including zero or more of the following.

=over 8

=item exclusive

If TRUE, then it will be presumed we have exclusive access to the database and certain
optimizations will be possible. The default is FALSE, meaning the data can change on us.

=item checkField

If specified, the name of an alternate key field that uniquely identifies entity instances.
This field can be used to determine if an entity instance already exists in the database.
The default is that no such field exists.

=back

=back

=cut

sub new {
    # Get the parameters
    my ($class, $entityName, $loader, $stats, %options) = @_;
    # This will be the return value.
    my $retVal;
    # Determine how to construct the object.
    if ($options{exclusive}) {
        require ERDB::ID::Counter::Exclusive;
        $retVal = ERDB::ID::Counter::Exclusive->new($entityName, $loader, $stats, %options);
    } else {
        require ERDB::ID::Counter::Shared;
        $retVal = ERDB::ID::Counter::Shared->new($entityName, $loader, $stats, %options);
    }
    # Specify a default allocation size and denote we have no IDs.
    $retVal->{nextID} = 1;
    $retVal->{lastID} = 0;
    $retVal->{allocation} = DEFAULT_ALLOCATION;
    # Return the object created.
    return $retVal;
}


=head2 Subclass Methods

=head3 NextID

    my $nextID = $helper->NextID;

Return the next available ID. This method is normally very simple. However, if we have
exceeded our allocation, it will need to go back to the database for more.

=cut

sub NextID {
    # Get the parameters.
    my ($self) = @_;
    # Get the statistics object.
    my $stats = $self->stats;
    # Get the next ID.
    my $retVal = $self->{nextID};
    # Is it available?
    if ($retVal > $self->{lastID}) {
        # No, we need to ask for more.
        my $entityName = $self->{entityName};
        $stats->Add($entityName . "IDRequests" => 1);
        # Compute the allocation size.
        my $allocation = $self->{allocation};
        # Make the request.
        $retVal = $self->db->AllocateIds($entityName, $allocation);
        # Record the new last ID.
        $self->{lastID} = $retVal + $allocation - 1;
        $stats->Add($entityName . "IDsAllocated" => $allocation);
        # Allocate the default next time.
        $self->{allocation} = DEFAULT_ALLOCATION;
    }
    # Denote we got this ID.
    $stats->Add($self->{entityName} . "IDsUsed" => 1);
    $self->{nextID} = $retVal + 1;
    # Return the ID found.
    return $retVal;
}


=head2 Virtual Overrides

=head3 SetEstimate

    $helper->SetEstimate($estimate);

Specify the expected number of inserts for this session. This helps to optimize certain types
of ID processing.

=over 4

=item estimate

The number of inserts of this entity type expected during the current session.

=back

=cut

sub SetEstimate {
    # Get the parameters.
    my ($self, $estimate) = @_;
    # Store the estimate.
    $self->{allocation} = $estimate;
}


1;