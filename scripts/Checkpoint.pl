#!/usr/bin/env perl
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


use strict;
use warnings;
use FIG_Config;
use Shrub;
use ScriptUtils;
use Shrub::Roles;

=head1 Checkpoint Shrub Data for Reloading

    Checkpoint.pl [ options ]

This script checkpoints Shrub data to the Inputs/Other directory for reloading. Currently, it is used
only for stabilizing role IDs.

=head2 Parameters

The command-line options are those found in L<Shrub/script_options> plus the following.

=over 4

=item repo

Location of the input repository. The default is C<Inputs> in the SEEDtk Data directory.

=back

=cut

# Get the command-line parameters.
my $opt = ScriptUtils::Opts('', Shrub::script_options(),
            ['repo|r=s', "location of the target repository", { default => "$FIG_Config::data/Inputs" }],
        );
# Connect to the database.
my $shrub = Shrub->new_for_script($opt);
# Compute the output file name.
my $outFile = $opt->repo . "/Other/roles.tbl";
# Write the output file.
Shrub::Roles::Checkpoint($shrub, $outFile);
print "All done. Roles written to $outFile.\n";
