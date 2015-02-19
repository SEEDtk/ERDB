#!/usr/bin/env perl

#
# Copyright (c) 2003-2006 University of Chicago and Fellowship
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

=head1 System Status Test Script

This script is used to debug problems with library includes. It displays the
PERL library search paths followed by a dump of a hash that displays where every
included module was found.

=cut

    use strict;
    use Shrub;
    use ScriptUtils;
    use Data::Dumper;

    print join("\n", @INC, "");
    print Dumper(\%INC);

