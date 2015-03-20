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
use ERDBtk::Utils;
use ScriptUtils;
use File::Copy::Recursive;
use Shrub::Functions;

=head1 Shrub Creation and Tuning Script

    ShrubTune [ options ]

This script performs various tuning and cleanup operations on a database.
Unlike most Shrub functions, it always uses an external DBD rather than
the DBD stored in the database. This means it is well-suited to adjusting
the database when the design changes.

=head2 Parameters

The command-line options are those found in L<Shrub/script_options> and
L<ERDBtk::Utils/init_options> plus the following.

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

=item fixfuns

Verify that each function is connected to its roles and has the correct description text.
Mutually exclusive with C<clear>.

=back

=cut

# Start timing.
my $startTime = time;
$| = 1; # Prevent buffering on STDOUT.
# Get the command parameters.
my $opt = ScriptUtils::Opts('', Shrub::script_options(), ERDBtk::Utils::init_options(),
    ['fixup|f', "fix existing tables to match the DBD"],
    ['missing|m', "create missing tables"],
    ['relfix|r=s@', "verify relationship (all to verify all)"],
    ['fixfuns|F', "verify the functions table"]
    );
# Validate the options.
if ($opt->clear) {
    if ($opt->fixup) {
        die "Cannot specify both \"clear\" and \"fixup\".";
    } elsif ($opt->missing) {
        die "Cannot specify both \"clear\" and \"missing\".";
    } elsif ($opt->fixfuns) {
        die "Cannot specify both \"clear\" and \"fixfuns\".";
    }
}
# Connect to the database and get the command parameters.
print "Connecting to the database.\n";
my $shrub = Shrub->new_for_script($opt, externalDBD => 1);
# Get the utility helper.
my $utils = ERDBtk::Utils->new($shrub);
# Get the statistics object.
my $stats = $utils->stats;
# Display the DBD.
print "Database definition taken from " . $shrub->GetMetaFileName() , ".\n";
# Process the initialization options.
my $cleared = $utils->Init($opt);
if ($cleared) {
    # If we cleared the database, erase the DNA repository.
    print "Erasing DNA repository.\n";
    File::Copy::Recursive::pathempty($FIG_Config::shrub_dna) ||
        die "Error clearing DNA repository: $!";
} else {
    # We still have a database. Check for DBD tuning options.
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
    # Check for a functions fix.
    if ($opt->fixfuns) {
        print "Function table will be verified.\n";
        FixFunctionTable($shrub, $stats);
    }
}
# Compute the total time.
my $timer = time - $startTime;
$stats->Add(totalTime => $timer);
# Tell the user we're done.
print "Database processed.\n" . $stats->Show();


=head2 Utility Subroutines

=head3 FixFunctionTable

    FixFunctionTable($shrub, $stats);

Verify the integrity of the function table. This implies insuring that each function is connected to all
its roles and the roles have their EC and TC numbers included.

=over 4

=item shrub

L<Shrub> object used to communicate with the datanase.

=item stats

L<Stats> object for keeping statistics about this operation.

=back

=cut

sub FixFunctionTable {
    # Get the parameters.
    my ($shrub, $stats) = @_;
    print "Reading role table.\n";
    my %roles = map { $_->[0] => Shrub::FormatRole($_->[1], $_->[2], $_->[3]) } $shrub->GetAll('Role', '', [], 'id ec-number tc-number description');
    my $roleCount = scalar keys %roles;
    print "$roleCount roles found.\n";
    $stats->Add(roleTable => $roleCount);
    # Get a map of the function-to-role connections.
    my %funRoles;
    map { push @{$funRoles{$_->[0]}}, $_->[1] } $shrub->GetAll('Function2Role', '', [], 'from-link to-link');
    # This will contain a list of the roles we couldn't find.
    my %missing;
    # Now loop through the functions.
    my $q = $shrub->Get('Function', '', [], 'id sep description');
    while (my $funData = $q->Fetch()) {
        $stats->Add(functionChecked => 1);
        # Get the function fields.
        my ($id, $sep, $description) = $funData->Values('id sep description');
        # Parse the function ID.
        if ($id =~ /\-/) {
            # Here we have a role-less function. Verify the separator.
            if ($sep ne '-') {
                $stats->Add(separatorFixed => 1);
                $shrub->UpdateEntity(Function => $id, sep => '-');
            }
            # Insure there are no roles connected.
            if (exists $funRoles{$id}) {
                my $deleted = $shrub->Disconnect('Function2Role', Function => $id);
                print "$deleted role connections removed from function $id: $description.\n";
                $stats->Add(roleConnectionsDeleted => $deleted);
            }
        } else {
            # Here the function has roles.
            my @subRoles = sort split /[\/\@;]/, $id;
            # Verify that the correct roles are connected.
            my @connectedL = sort @{$funRoles{$id}};
            # Compare the connected roles with the roles in the ID.
            my $subI = 0;
            my $subRole = $subRoles[$subI];
            my $connected = shift @connectedL;
            while (defined $subRole || defined $connected) {
                if (! defined $connected || $subRole lt $connected) {
                    # Here we have an unconnected role.
                    print "Connecting $subRole to $id.\n";
                    $shrub->InsertObject('Function2Role', 'from-link' => $id, 'to-link' => $subRole);
                    $stats->Add(roleConnectionsAdded => 1);
                    # Get the next sub-role.
                    $subRole = $subRoles[++$subI];
                } elsif (! defined $subRole || $subRole gt $connected) {
                    # Here we have an extra connection.
                    print "Disconnecting $connected from $id.\n";
                    $shrub->DeleteRow('Function2Role', $id, $connected);
                    $stats->Add(roleConnectionsDeleted => 1);
                    # Get the next connected role.
                    $connected = shift @connectedL;
                } else {
                    # Here both roles match.
                    $stats->Add(roleConnectionCorrect => 1);
                    $subRole = $subRoles[++$subI];
                    $connected = shift @connectedL;
                }
            }
            # Now compute the function text.
            my $realSep = ($sep eq ';' ? '; ' : " $sep ");
            my @roleNames;
            for my $subRole (@subRoles) {
                my $roleName = $roles{$subRole};
                if (! $roleName) {
                    $missing{$subRole}++;
                    $stats->Add(missingRole => 1);
                    print "Function $id refers to missing role $subRole.\n";
                } else {
                    push @roleNames, $roleName;
                }
            }
            my $realDescription = join($realSep, @roleNames);
            if ($realDescription ne $description) {
                $shrub->UpdateEntity(Function => $id, description => $realDescription);
                $stats->Add(descriptionUpdated => 1);
            }
        }
    }
    # Now add the missing roles to the statistics object.
    for my $missingRole (sort keys %missing) {
        $stats->AddMessage("$missingRole is missing from the Role table.");
    }
}
