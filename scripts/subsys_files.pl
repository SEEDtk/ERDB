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
use File::Copy::Recursive;

=head1 Create the Subsystem Projection Files

    subsys_files.pl [ options ] outDir

This script creates the files C<roleMap.tbl> and C<variantMap.tbl> in the specified output directory.
These files are used by the L<SubsystemProjector> object to project subsystems in the absence of the
Shrub database.

The C<roleMap.tbl> file maps roles to subsystem names. Each record contains a role checksum followed by
a subsystem name.

The C<variantMap.tbl> file contains role lists for subsystem variants. Each record contains (0) a subsystem
name, (1) a variant code, and (2) a space-delimited list of role checksums.

Both files are tab-delimited.

=head2 Parameters

The positional parameter is the name of the directory to contain the output files.

The command-line options are those found in L<Shrub/script_options> plus the following.

=over 4

=item subFile

If specified, a list of subsystem IDs and subsystem names. The subsystem names will be modified to the
specified value. A name of C<delete> will cause the subsystem to be skipped.

=item badVariants

If specified, a list of variant codes that should not be included in the output, one per line.

=back

=cut

$| = 1;
# Get the command-line parameters.
my $opt = ScriptUtils::Opts('outDir',
        Shrub::script_options(),
        ['subFile|subfile|S=s', 'file containing subsystem renames'],
        ['badVariants|badvariants|bV|bv=s', 'file containing list of bad variant codes']
        );
my $stats = Stats->new();
# Connect to the database.
my $shrub = Shrub->new_for_script($opt);
# Get the output directory.
my ($outDir) = @ARGV;
if (! $outDir) {
    die "No output directory specified.";
} elsif (! -d $outDir) {
    File::Copy::Recursive::pathmk($outDir) || die "Could not create output directory: $!";
}
# Check for subsystem renames.
my %subMap;
if ($opt->subfile) {
    open(my $ih, '<', $opt->subfile) || die "Could not open subsystem rename file: $!";
    while (! eof $ih) {
        my $line = <$ih>;
        if ($line =~ /^\S+\t.+/) {
            $subMap{$1} = $2;
            $stats->Add(subsystemMapping => 1);
        }
    }
}
# Check for bad variants.
my %badVariants;
if ($opt->badvariants) {
    open(my $ih, '<', $opt->badvariants) || die "Could not open bad-variants file: $!";
    while (! eof $ih) {
        my $line = <$ih>;
        chomp $line;
        $badVariants{$line} = 1;
        $stats->Add(badVariantCode => 1);
    }
}
# Open the output files.
open(my $rh, ">$outDir/roleMap.tbl") || die "Could not open roleMap: $!";
open(my $vh, ">$outDir/variantMap.tbl") || die "Could not open variantMap: $!";
# Loop through the subsystems.
my $q = $shrub->Get('Subsystem', '', [], 'id name');
while (my $subRow = $q->Fetch()) {
    $stats->Add(subsystems => 1);
    my ($id, $name) = $subRow->Values('id name');
    print "Processing $id: $name\n";
    if ($subMap{$id}) {
        $name = $subMap{$id};
        if ($name ne 'delete') {
            $stats->Add(mappedSubsytem => 1);
            print "Mapped name is $name.\n";
        }
    }
    # Check for deletion.
    if ($name eq 'delete') {
        print "Skipping subsystem-- deleted by map file.\n";
        $stats->Add(skippedSubsystem => 1);
    } else {
        # Get all of the subsystem's roles. We need the role checksums and a hash mapping IDs to checksums.
        # This last is used to translate the variant maps.
        my %roles = map { $_->[0] => $_->[1] } $shrub->GetAll('Subsystem2Role Role', 'Subsystem2Role(from-link) = ?',
                [$id], 'Role(id) Role(checksum)');
        for my $role (sort keys %roles) {
            print $rh "$roles{$role}\t$name\n";
            $stats->Add(roleOut => 1);
        }
        # Get all of the subsystem's variants.
        my @variants = $shrub->GetAll('Subsystem2Map VariantMap', 'Subsystem2Map(from-link) = ?', [$id],
                'VariantMap(variant-code) VariantMap(map)');
        for my $variant (@variants) {
            my ($code, $map) = @$variant;
            if ($badVariants{$code}) {
                print "Skipping map for variant $code.\n";
                $stats->Add(skippedMap => 1);
            } else {
                my @mapRoles = split /\s+/, $map;
                $map = join(' ', map { $roles{$_} } @mapRoles);
                print $vh "$name\t$code\t$map\n";
                $stats->Add(mapOut => 1);
            }
        }
    }
}
print "All done: " . $stats->Show();