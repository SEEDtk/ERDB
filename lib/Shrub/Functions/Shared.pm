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


package Shrub::Functions::Shared;

    use strict;
    use warnings;
    use base qw(Shrub::Functions);

=head1 Shrub Function Manager (Shared Database Access)

This is a version of the function manager designed for situations where we have exclusive
access to the database. This allows us to pre-load the functions into memory. Note that
if you are only doing a small number of updates, the shared-mode access might be faster.
This mode is more appropriate to bulk updates.

In addition to the fields in the base class object, this object contains the following fields.

=over 4

## field list

=back

=head2 Special Methods

=head3 init

    Shrub::Functions::Exclusive::init($funMgr, $loader, %options);

Initialize and bless a new function manager for exclusive mode.

=over 4

=item funMgr

A partially-constructed version of this object.

=item loader

L<Shrub::DBLoader> object for inserting into the database.

=item options

A hash of options. Currently, none are used by this subclass.

=back

=cut

sub init {
    # Get the parameters.
    my ($self, $loader, %options) = @_;
    ## TODO constructor code for Shrub::Functions::Exclusive
    # Bless the object to make it real.
    bless $self, __PACKAGE__;
}

=head2 Virtual Overrides

=cut

## TODO virtual methods for Shrub::Functions::Exclusive

1;