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
use Stats;

=head1 Reset Questionable-Genome Flags

    UpdateQuestionables.pl [ options ]

This script updates the well-behaved flags based on a new list of questionable genomes.
Genomes in the file that are currently marked well-behaved will have their well-behaved
bits cleared.

=head2 Parameters

There are no positional parameters.

The command-line options are those found in L<Shrub/script_options>.

=cut

# Get the command-line parameters.
my $opt = ScriptUtils::Opts('oldFile',
        Shrub::script_options(),
        );
# Start a statistics object.
my $stats = Stats->new();
# Connect to the database.
my $shrub = Shrub->new_for_script($opt);
# Open the input file.
open(my $ih, "<$FIG_Config::data/Inputs/Other/questionables.tbl") || die "Could not open questionables.tbl: $!";
# Loop through it.
while (! eof $ih) {
    my $line = <$ih>;
    $stats->Add(lineIn => 1);
    if ($line =~ /^(\d+\.\d+)\s/) {
        my $genome = $1;
        $stats->Add(genomeIn => 1);
        my ($oldValue) = $shrub->GetFlat('Genome', 'Genome(id) = ?', [$genome], 'well-behaved');
        if (! defined $oldValue) {
            print "Genome $genome not in database.\n";
            $stats->Add(genomeNotFound => 1);
        } elsif ($oldValue == 0) {
            print "Genome $genome already questionable.\n";
            $stats->Add(genomeAlreadyUnset => 1);
        } else {
            print "Updating $genome.\n";
            $stats->Add(genomeUpdated => 1);
            $shrub->UpdateEntity(Genome => $genome, 'well-behaved' => 0);
        }
    }
}
print "All done: " . $stats->Show();
