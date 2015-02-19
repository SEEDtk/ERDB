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


package CopyFromSeed;

    use strict;
    use warnings;
    use base qw(Loader);

=head1 CopyFromSeed Helper Object

This is a helper object that manages the data structures needed by L<CopyFromSeed.pl>.
Rather than pass dozens of parameters to each major subroutine, we simply pass around
this object. It contains the following fields.

=over 4

=item figDisk

Name of the SEED FIGdisk containing the source data for the copy.

=item opt

The L<Getopt::Long::Descriptive::Opts> object containing the command-line options.

=item functionMap

A reference to a hash mapping feature IDs to functions.

=item genomesCopied

A reference to a hash containing the ID of each genome that has been copied
from the SEED to the output.

=item genomesFunctioned

A reference to a hash containing the ID of each genome whose features are in
the function map.

=item genomeOutput

Name of the output directory for genomes, or C<undef> if genomes are not to
be copied.

=item subsysOutput

Name of the output directory for subsystems, or C<undef> if subsystems are
not to be copied.

=back

=head2 Command-Line Option Groups

=head3 subsys_options

    my @opt_specs = CopyFromSeed::subsys_options();

These are the command-line options relating to copying of subsystems.

=over 4

=item subsysDir

The path to the folder in which the exchange-format directories of
copied subsystems should be placed. The default is the default
subsystem input repository (C<$FIG_Config::shrub_dir/Inputs/SubSystemData>).

=item subsystems

If specified, the name of a file containing a list of the names
of the subsystems to copy. If omitted, all subsystems in the
specified SEED will be copied. If C<none>, no subsystems will
be copied.

=item subgenomes

If specified, all genomes in the spreadsheets of the specified subsystems
will be copied.

=item subpriv

If specified, the subsystems copied will be marked as privileged; that is,
only privileged users will be able to edit them.

=back

=cut

sub subsys_options {
    return (
            ["subsysDir|s=s", "output directory for subsystem folders", { default => "$FIG_Config::shrub_dir/Inputs/SubSystemData"}],
            ["subsystems=s", "file listing subsystems to copy (default all)"],
            ["subgenomes", "if specified, all genomes in the spreadsheets of the specified subsystems will be copied"],
            ["subpriv", "if specified, the subsystems copied will be treated as privileged"],
    );
}

=head3 genome_options

    my @opt_spec = CopyFromSeed::genome_options();

These are the command-line options relating to the copying of genomes.

=over 4

=item genomeDir

The path to the folder in which the exchange-format directories of copied
genomes should be placed. The default is the default genome input repository
(C<$FIG_Config::shrub_dir/Inputs/GenomeData>).

=item genomes

If specified, a file containing a list of the genome IDs for the genomes to
copy. If omitted, all genomes in the specified SEED will be copied. If
C<none>, no genomes will be copied.

=item blacklist

If specified, the name of a file containing a list of IDs for genomes that should
not be copied. This overrides all other parameters.

=item proks

If specified, only prokaryotic genomes will be copied.

=item genpriv

The privilege level of the genome data being copied-- 0 (public), 1 (projected), or
2 (privileged).

=back

=cut

sub genome_options {
    return (
            ["genomeDir|g=s", "output directory for genome folders", { default => "$FIG_Config::shrub_dir/Inputs/GenomeData"}],
            ["genomes=s", "file listing genomes to copy (default all)"],
            ["proks", "if specified, only prokaryotic genomes will be copied"],
            ["blacklist=s", "the name of a file containing IDs of genomes that should not be copied"],
            ["genpriv=i", "privilege level of the genomes being copied-- 0 (public), 1 (projected), or 2 (privileged)"],
    );
}

=head2 Special Methods

=head3 new

    my $helper = CopyFromSeed->new($opt, $figDisk);

Construct a new helper object with the specified command-line options and SEED FIGdisk.

=over 4

=item opt

L<Getopt::Long::Descriptive::Opts> object containing the command-line options.

=item figDisk

Name of the SEED FIGdisk directory from which the genome and subsystem data is being
copied.

=back

=cut

sub new {
    # Get the parameters.
    my ($class, $opt, $figDisk) = @_;
    # Create the base-class object.
    my $retVal = Loader::new($class);
    # Store the FIGdisk pointer and command-line options.
    $retVal->{opt} = $opt;
    $retVal->{figDisk} = $figDisk;
    # Validate the FIGdisk.
    if (! $figDisk) {
        die "A SEED FIGdisk location is required.";
    } elsif (! -d $figDisk) {
        die "SEED FIGdisk location $figDisk is invalid or not found.";
    } elsif (! -d "$figDisk/FIG/Data/Organisms" || ! -d "$figDisk/FIG/Data/Subsystems") {
        die "Directory $figDisk does not appear to be a FIGdisk directory.";
    }
    # Create the tracking hashes.
    $retVal->{functionMap} = {};
    $retVal->{genomesCopied} = {};
    $retVal->{genomesFunctioned} = {};
    # Get the output directories. We use hash notation rather than the member facility of $opt
    # in case the option was not defined by the client. (For example, a genome copy script would
    # not have "subsysDir".)
    $retVal->{genomeOutput} = $opt->{genomedir};
    $retVal->{subsysOutput} = $opt->{subsysdir};
    # Return the created object.
    return $retVal;
}


=head2 Public Manipulation Methods

=head3 ClearFunctionMap

    $loader->ClearFunctionMap();

Erase the function map to relieve memory.

=cut

sub FreeFidFunctions {
    my ($self) = @_;
    $self->{functionMap} = {};
    $self->{genomesFunctioned} = {};
}

1;