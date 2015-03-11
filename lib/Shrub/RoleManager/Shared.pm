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


package Shrub::RoleManager::Shared;

    use strict;
    use warnings;
    use ERDB::ID::Magic::Shared;

=head1 Shrub Role Manager

This object manages insertion of roles into the database. The process is complicated because a new
role may have an EC number that needs to be stored in the existing roles. The object has both
exclusive and shared operating modes.

=head2 Special Methods

=cut

## TODO RoleManager new



## TODO RoleManager insert

1;
