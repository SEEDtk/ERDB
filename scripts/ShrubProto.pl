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
    use Shrub;
    use ShrubLoader;

=head1 Prototype Shrub Load Script

    ShrubProto [ options ] parm1 parm2 ...

This is a prototype template for a database load script.

=head2 Parameters

## describe positional parameters

The command-line options are those found in L<Shrub/new_for_script> plus
the following.

=over 4

## more command-line options

=back

=cut

    # Start timing.
    my $startTime = time;
    $| = 1; # Prevent buffering on STDOUT.
    # Connect to the database and get the command parameters.
    print "Connecting to the database.\n";
    my ($shrub, $opt) = Shrub->new_for_script('%c %o parm1 parm2 ...', { },
            ## more command-line options here
            );
    # Get the load helper.
    my $loader = ShrubLoader->new($shrub);
    # Get the statistics object.
    my $stats = $loader->stats;

    ## do the loading

    # Close and upload the load files.
    print "Unspooling load files.\n";
    $loader->Close();
    # Compute the total time.
    my $timer = time - $startTime;
    $stats->Add(totalTime => $timer);
    # Tell the user we're done.
    print "Database processed.\n" . $stats->Show();
