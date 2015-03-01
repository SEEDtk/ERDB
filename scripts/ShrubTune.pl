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
use ERDB::Utils;
use ScriptUtils;

=head1 Shrub Creation and Tuning Script

    ShrubTune [ options ]

This script performs various tuning and cleanup operations on a database.
Unlike most Shrub functions, it always uses an external DBD rather than
the DBD stored in the database. This means it is well-suited to adjusting
the database when the design changes.

=head2 Parameters

The command-line options are those found in L<Shrub/script_options> and
L<ERDB::Utils/init_options> plus the following.

=over 4

=item fixup

Fix all existing tables in the database to match the DBD. It is not currently possible
to fix everything, but this method will do as much as possible. Mutually exclusive with
C<clear>.

=item missing

Create any tables that are missing. Mutually exclusive with C<clear>.

=item relfix

Verify the specified relationship to insure it connects correctly to the entities on both
sides. This parameter can be specified more than once to process more than one relationship.
If the value is C<all>, all relationships will be verified, which can take an extremely long
time.

=back

=cut

# Start timing.
my $startTime = time;
$| = 1; # Prevent buffering on STDOUT.
# Get the command parameters.
my $opt = ScriptUtils::Opts('', Shrub::script_options(), ERDB::Utils::init_options(),
    ['fixup|f', "fix existing tables to match the DBD"],
    ['missing|m', "create missing tables"],
    ['relfix|r=s@', "verify relationship (all to verify all)"]
    );
# Validate the options.
if ($opt->clear) {
    if ($opt->fixup) {
        die "Cannot specify both \"clear\" and \"fixup\".";
    } elsif ($opt->missing) {
        die "Cannot specify both \"clear\" and \"missing\".";
    }
}
# Connect to the database and get the command parameters.
print "Connecting to the database.\n";
my $shrub = Shrub->new_for_script($opt, externalDBD => 1);
# Get the utility helper.
my $utils = ERDB::Utils->new($shrub);
# Get the statistics object.
my $stats = $utils->stats;
# Process the initialization options.
my $cleared = $utils->Init($opt);
if (! $cleared) {
    # We still have a database. Check for tuning options.
    if ($opt->fixup) {
        # Fix up the existing tables.
        my $badTables = $utils->FixDatabase();
        if ($badTables) {
            print "$badTables tables could not be fixed.\n";
        }
    }
    if ($opt->missing) {
        # Create missing tables.
        $utils->CreateMissing();
    }
    # Get the list of relationships to verify.
    my $rels = $opt->relfix // [];
    if ($rels->[0] && $rels->[0] eq 'all') {
        # Here the user wants all the relationships.
        my $relH = $shrub->GetObjectsTable('relationship');
        $rels = [ sort keys %$relH ];
        print "All relationships will be verified.\n";
    }
    for my $rel (@$rels) {
        $utils->FixRelationship($rel);
    }
}
# Compute the total time.
my $timer = time - $startTime;
$stats->Add(totalTime => $timer);
# Tell the user we're done.
print "Database processed.\n" . $stats->Show();
