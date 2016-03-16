#!/usr/bin/perl -w

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


package Shrub;

    use strict;
    use FIG_Config;
    use StringUtils;
    use base qw(ERDBtk);
    use Stats;
    use DBtk;
    use SeedUtils;
    use Digest::MD5;
    use Shrub::Roles;
    use gjoseqlib;
    use BasicLocation;


=head1 Shrub Database Package

=head2 Introduction

The Shrub database is a new Entity-Relationship Database that implements
the repository for the SEEDtk system. This object has minimal
capabilities: most of its power comes the L<ERDBtk> base class.

The fields in this object are as follows.

=over 4

=item loadDirectory

Name of the directory containing the files used by the loaders.

=item dnaRepo

Name of the directory containing the DNA repository.

=back

=head2 Special Methods

=head3 new

    my $shrub = Shrub->new(%options);

Construct a new Shrub object. The following options are supported.

=over 4

=item loadDirectory

Data directory to be used by the loaders.

=item DBD

XML database definition file.

=item dbName

Name of the database to use.

=item sock

Socket for accessing the database.

=item userData

Name and password used to log on to the database, separated by a slash.

=item dbhost

Database host name.

=item port

MYSQL port number to use (MySQL only).

=item dbms

Database management system to use (e.g. C<SQLite> or C<postgres>, default C<mysql>).

=item dnaRepo

Name of the directory containing the DNA repository.

=item offline

If TRUE, then the database object will be built, but there will be no
connection made to the database. The default is FALSE.

=item externalDBD

If TRUE, then the external database definition (DBD) XML file will override whatever is stored
in the database. This is implied if B<DBD> is specified.

=back

=cut

sub new {
    # Get the parameters.
    my ($class, %options) = @_;
    # Compute the default base directory.
    my $dirBase = $FIG_Config::data || '/vol/seedtk/shrub';
    # Get the options.
    if (! $options{loadDirectory}) {
        $options{loadDirectory} = "$dirBase/LoadFiles";
    }
    my $dbd = $options{DBD} || $FIG_Config::shrub_dbd || "$dirBase/ShrubDBD.xml";
    my $dbName = $options{dbName} || $FIG_Config::shrubDB || "seedtk_shrub";
    my $userData = $options{userData} || $FIG_Config::userData || "seed/";
    my $dbhost = $options{dbhost} || $FIG_Config::dbhost || "seed-db-write.mcs.anl.gov";
    my $dnaRepo = $options{dnaRepo} || "$FIG_Config::shrub_dna";
    my $port = $options{port} || $FIG_Config::dbport || 3306;
    my $dbms = $options{dbms} || 'mysql';
    # Insure that if the user specified a DBD, it overrides the internal one.
    if ($options{DBD} && ! defined $options{externalDBD}) {
        $options{externalDBD} = 1;
    }
    # Compute the socket. An empty string is a valid override here.
    my $sock = $options{sock} || "";
    # Compute the user name and password.
    my ($user, $pass) = split '/', $userData, 2;
    $pass = "" if ! defined $pass;
    # Connect to the database, if desired.
    my $dbh;
    if (! $options{offline}) {
         $dbh = DBtk->new($dbms, $dbName, $user, $pass, $port, $dbhost, $sock);
    }
    # Create the ERDBtk object.
    my $retVal = ERDBtk::new($class, $dbh, $dbd, %options);
    # Attach the repository pointer.
    $retVal->{dnaRepo} = $dnaRepo;
    # Return it.
    return $retVal;
}

=head3 new_for_script

    my $shrub = Shrub->new_for_script($opt, %tuning);

Construct a new Shrub object for a command-line script.

=over 4

=item opt

An options object from L<Getopt::Long::Descriptive> that
includes the parameters from L</script_options>.

=item tuning

A hash with the following (optional) members.

=over 8

=item externalDBD

If TRUE, use of an external DBD will be forced, overriding the DBD stored in the database.

=item offline

If TRUE, the database object will be constructed but not connected to the database.

=back

=back

=cut

sub new_for_script {
    # Get the parameters.
    my ($class, $opt, %tuning) = @_;
    # Check for an external DBD override.
    my $externalDBD = $tuning{externalDBD} || $opt->dbd;
    # Here we have a real invocation, so we can create the Shrub object.
    my $retVal = Shrub::new($class, loadDirectory => $opt->loaddirectory, DBD => $opt->dbd,
            dbName => $opt->dbname, sock => $opt->sock, userData => $opt->userdata,
            dbhost => $opt->dbhost, port => $opt->port, dbms => $opt->dbms,
            dnaRepo => $opt->dnarepo, offline => $tuning{offline},
            externalDBD => $externalDBD
            );
    # Return the result.
    return $retVal;
}

=head3 script_options

    my @opt_specs = Shrub::script_options();

These are the command-line options for connecting to a Shrub database.

=over 4

=item loadDirectory

Data directory to be used by the loaders.

=item DBD

XML database definition file.

=item dbName

Name of the database to use.

=item sock

Socket for accessing the database.

=item userData

Name and password used to log on to the database, separated by a slash.

=item dbhost

Database host name.

=item port

MYSQL port number to use (MySQL only).

=item dbms

Database management system to use (e.g. C<postgres>, default C<mysql>).

=item dnaRepo

Name of the directory containing the DNA repository.

=back

This method returns the specifications for these command-line options in a form
that can be used in the L<ScriptUtils/Opts> method.

=cut

sub script_options {
    return (
           [ "loadDirectory=s", "directory for creating table load files" ],
           [ "DBD=s", "file containing the database definition XML" ],
           [ "dbName=s", "database name" ],
           [ "sock=s", "MYSQL socket" ],
           [ "userData=s", "name/password for database logon" ],
           [ "dbhost=s", "database host server" ],
           [ "port=i", "mysql port" ],
           [ "dbms=s", "database management system" ],
           [ "dnaRepo=s", "DNA repository directory root" ],
    );
}

=head2 Data Query Methods

=head3 contig_seek

    my $dna = $shrub->contig_seek($contigID);

Return the DNA sequence for a contig. This method finds the relevant FASTA file and pulls the contig's DNA sequence
from it.

=over 4

=item contigID

ID of the contig whose DNA sequence is desired.

=item RETURN

Returns the DNA sequence for the contig, or C<undef> if the contig does not exist.

=back

=cut

sub contig_seek {
    my ($self, $contigID) = @_;
    # This will be the return value.
    my $retVal;
    # Get the name of the FASTA file.
    my $fastaDir = $self->DNArepo;
    my ($fastaFile) = $self->GetFlat('Contig2Genome Genome', 'Contig2Genome(from-link) = ?', [$contigID], 'Genome(contig-file)');
    if ($fastaFile) {
        my $fileName = "$fastaDir/$fastaFile";
        # Open the FASTA file for input.
        open(my $fh, "<$fileName") || die "Could not open contig fasta file $fastaFile: $!";
        # Find the contig.
        my $found;
        while (! eof $fh && ! $found) {
            my $line = <$fh>;
            if ($line =~ /^>(\S+)/) {
                $found = ($1 eq $contigID);
            }
        }
        # If we found the contig, assemble the DNA.
        if ($found) {
            my @dna;
            while (! eof $fh && $found) {
                my $line = <$fh>;
                if (substr($line, 0, 1) eq '>') {
                    $found = 0;
                } else {
                    chomp $line;
                    push @dna, $line;
                }
            }
            $retVal = join("", @dna);
        }
    }
    # Return the DNA sequence.
    return $retVal;
}

=head3 Feature2Function

    my $featureMap = $shrub->Feature2Function($priv, \@features);

Get the functions assigned to each of the specified features at the specified privilege level.

=over 4

=item priv

Privilege level of interest (C<0> for unprivileged, C<1> for projected, C<2> for privileged).

=item features

Reference to a list of feature IDs.

=item RETURN

Returns a reference to a hash mapping each feature to a 3-tuple. Each 3-tuple will consist of
(0) the function ID, (1) the function description, and (2) the associated comment.

=back

=cut

sub Feature2Function {
    # Get the parameters.
    my ($self, $priv, $features) = @_;
    # The return hash will be built in here.
    my %retVal;
    # Loop through the features.
    for my $feature (@$features) {
        # We'll store the function data in here.
        my $functionData;
        ($functionData) = $self->GetAll('Feature2Function Function',
                'Feature2Function(from-link) = ? AND Feature2Function(security) = ?',
                [$feature, $priv], 'Function(id) Function(description) Feature2Function(comment)');
        # Store the function data in the return hash.
        $retVal{$feature} = $functionData;
    }
    # Return the computed hash.
    return \%retVal;
}


=head3 Feature2Trans

    my $featureMap = $shrub->Feature2Trans(\@features);

Get the translation assigned to each of the specified features

=over 4

=item features

Reference to a list of feature IDs.

=item RETURN

Returns a reference to a hash mapping each feature to  the translation

=back

=cut

sub Feature2Trans {
    # Get the parameters.
    my ($self, $features) = @_;
    # The return hash will be built in here.
    my %retVal;
    # Loop through all the features.
    for my $feature (@$features) {
        # We'll store the function data in here.
        my $Translation;
        ($Translation) = $self->GetFlat('Feature Protein ',
                '(Feature(id) = ?)',
                [$feature],
                "Protein(sequence)");
        # Store the translation in the return hash.
        $retVal{$feature} = $Translation;
    }
    # Return the computed hash.
    return \%retVal;
}
=head3 Subsystem2Feature

    my $fidList = $shrub->Subsystem2Feature($sub);

Return a list of all the features in a single subsystem.

=over 4

=item sub

The name or ID of the subsystem of interest.

=item RETURN

Returns a reference to a list of the feature IDs for al the features that have
been populated in the subsystem.

=back

=cut

sub Subsystem2Feature {
    # Get the parameters.
    my ($self, $sub) = @_;
    # Read the subsystem features from the database. NOTE that right now the ID and name
    # are the same field.
    my @retVal = $self->GetFlat('Subsystem2Row Row2Cell Cell2Feature', "Subsystem2Row(from-link) = ?", [$sub],
            'Cell2Feature(to-link)');
    # Return the result.
    return \@retVal;
}


=head3 FeaturesInRegion

    my @tuples = $shrub->FeaturesInRegion($contigID, $start, $end);

Return a list of the features in the specified contig region. For each such feature, this method will
return the feature ID, leftmost location, and direction for the segment that overlaps the region.

=over 4

=item contigID

ID of the contig (including the genome ID) containing the region of interest.

=item start

Index of the location in the contig where the region of interest begins.

=item end

Index of the location in the contig where the region of interest ends.

=item RETURN

Returns a list of 5-tuples, each consisting of (0) a feature ID, (1) the index of the leftmost
location in the segment that overlaps the region, and (2) the direction of the segment that
overlaps the region, (3) the segment length, and (4) the total feature length.

=back

=cut

sub FeaturesInRegion {
    # Get the parameters.
    my ($self, $contigID, $start, $end) = @_;
    my ($limit) = $self->GetFlat('Contig2Genome Genome', 'Contig2Genome(from-link) = ?', [$contigID],
            'Genome(longest-feature)');
    # Every feature that overlaps MUST start to the left of this point.
    my $leftLimit = $start - $limit;
    # Request the desired tuples.
    my @retVal = $self->GetAll("Contig2Feature Feature",
                          'Contig2Feature(from-link) = ? AND (Contig2Feature(begin) >= ? AND Contig2Feature(begin) <= ? OR Contig2Feature(begin) >= ? AND Contig2Feature(begin) < ? AND Contig2Feature(begin) + Contig2Feature(len) >= ?)',
                          [$contigID, $start, $end, $leftLimit, $start, $start],
                          [qw(Feature(id) Contig2Feature(begin) Contig2Feature(dir) Contig2Feature(len) Feature(sequence-length))]);
    # Return them.
    return @retVal;
}

=head3 Subsystem2Role

    my @roles = $shrub->Subsystem2Role($sub);

Return all the roles in a subsystem, in order. For each role, we return
the ID and the description with the EC and TC numbers suffixed.

=over 4

=item sub

ID of the subsystem whose roles are desired.

=item RETURN

Returns a list of 2-tuples, each containing (0) a role ID, and (1) a role description, with the EC and TC numbers
included. The roles will be presented in their order within the subsystem.

=back

=cut

sub Subsystem2Role {
    # Get the parameters.
    my ($self, $sub) = @_;
    # Request the role data.
    my @retVal = map { [$_->[0], FormatRole($_->[1], $_->[2], $_->[3])] }
            $self->GetAll('Subsystem2Role Role', 'Subsystem2Role(from-link) = ? ORDER BY Subsystem2Role(ordinal)', [$sub],
            'Role(id) Role(ec-number) Role(tc-number) Role(description)');
    # Return the result.
    return @retVal;
}


=head3 FunctionName

    my $fname = $shrub->FunctionName($fun);

Return the description associated with a function ID.

=over 4

=item fun

ID of the function whose description is desired.

=item RETURN

Returns the function description, or the incoming function ID if it is not found.

=back

=cut

sub FunctionName {
    # Get the parameters.
    my ($self, $fun) = @_;
    # Declare the return variable.
    my ($retVal) = $self->GetFlat('Function', 'Function(id) = ?', [$fun], 'description');
    # Default to the incoming ID.
    $retVal //= $fun;
    # Return the result.
    return $retVal;
}

=head3 role_id_to_desc

    my $fname = $shrub->role_id_to_desc($role);

Return the description associated with a role ID.

=over 4

=item role

ID of the role whose description is desired.

=item RETURN

Returns the role description, or the incoming role ID if it is not found.

=back

=cut

sub role_id_to_desc {
    # Get the parameters.
    my ($self, $role) = @_;
    # Declare the return variable.
    my ($retVal) = $self->GetFlat('Role', 'Role(id) = ?', [$role], 'description');
    # Default to the incoming ID.
    $retVal //= $role;
    # Return the result.
    return $retVal;
}

=head3 all_genomes

    my $genomeH = $shrub->all_genomes($core);

or

    my @genomes = $shrub->all_genomes($core);

Return a hash mapping the ID of every genome in the database to its name or a list of genome IDs,
optionally restricted to only core genomes.

=over 4

=item core (optional)

If TRUE, then only core genomes are returned. The default is FALSE.

=item RETURN

Returns a reference to a hash mapping each genome's ID to its name.

=back

=cut

sub all_genomes {
    # Get the parameters.
    my ($self, $core) = @_;
    # Create the filter.
    my ($filter, $parms) = ('', []);
    if ($core) {
        $filter = 'Genome(core) = ?';
        $parms = [1];
    }
    # Get the genome data.
    my @genomes = $self->GetAll('Genome', $filter, $parms, 'id name');
    # Return it in the desired format.
    if (wantarray()) {
        return map { $_->[0] } @genomes;
    } else {
        my %retVal = map { $_->[0] => $_->[1] } @genomes;
        return \%retVal;
    }
}

=head3 fid_locs

    my @locs = $shrub->fid_locs($fid);

Return a list of L<BasicLocation> objects for the locations occupied by a specified feature.

=over

=item fid

ID of the relevant feature.

=item RETURN

Returns a list of L<BasicLocation> objects, one for each segment of the feature.

=back

=cut

sub fid_locs {
    # Get the parameters.
    my ($self, $fid) = @_;
    # Get the location information.
    my @locData = $self->GetAll('Feature2Contig', 'Feature2Contig(from-link) = ? ORDER BY Feature2Contig(from-link), Feature2Contig(ordinal)',
            [$fid], 'Feature2Contig(to-link) Feature2Contig(begin) Feature2Contig(dir) Feature2Contig(len)');
    # Convert to location objects.
    my @retVal = map { BasicLocation->new(\@$_) } @locData;
    # Return the result.
    return @retVal;
}

=head3 loc_of

    my $loc = $shrub->loc_of($fid);

Return the location of a feature. The feature is presumed to be on a single contig. If it has multiple locations,
a location will be formed from the leftmost and rightmost points on the contig, and the direction will be set to
the most common direction.

=over 4

=item fid

ID of the relevant feature.

=item RETURN

Returns a L<BasicLocation> object giving the overall location of the feature.

=back

=cut

sub loc_of {
    # Get the parameters.
    my ($self, $fid) = @_;
    # Get the location information.
    my @locs = $self->fid_locs($fid);
    # If there is only one, simply return it.
    my $retVal = pop @locs;
    if (@locs) {
        # This will track the directions.
        my %dirs = ($retVal->Dir => 1);
        for my $loc (@locs) {
            if ($loc->Contig eq $retVal->Contig) {
                $retVal->Merge($loc);
                $dirs{$loc->Dir}++;
            }
        }
        # If the other direction is more popular, flip the location.
        my $otherDirCount = $dirs{($retVal->Dir eq '-') ? '+' : '-'} // 0;
        if ($otherDirCount > $dirs{$retVal->Dir}) {
            $retVal->Reverse;
        }
    }
    # Return the result.
    return $retVal;
}


=head3 subsystem_to_role

    my \@tuples = $shrub->subsystem_to_role($subsys);

Return a list of the roles in a specified subsystem. For each role, we return the abbreviation
(which is specific to the subsystem), the ID, and the description.

=over 4

=item subsys

The ID of the subsystem in question.

=item RETURN

Returns a reference to a list of 3-tuples, one for each role in the subsystem, each tuple containing
(0) the role abbreviation, (1) the role ID, and (2) the role description text.

=back

=cut

sub subsystem_to_roles {
    my($self, $subsys) = @_;

    my @tuples = $self->GetAll("Subsystem2Role Role",
                                "Subsystem2Role(from-link) = ?", [$subsys],
                                "Subsystem2Role(abbr) Role(id) Role(description)");
    return \@tuples;
}

=head3 subsystem_to_rows

    my \@tuples = $shrub->subsystem_to_rows($subsys);

Return a list of the rows in a specified subsystem. For each row, we return the ID and the variant code.

=over 4

=item subsys

The ID of the subsystem whose rows are desired.

=item RETURN

Returns a reference to a list of 2-tuples, one per row, each tuple consisting of (0) the row ID, and (1) the
variant code for the row.

=back

=cut

sub subsystem_to_rows {
    my($self, $subsys) = @_;

    my @tuples = $self->GetAll("Subsystem2Row SubsystemRow",
                                "Subsystem2Row(from-link) = ?", [$subsys],
                                "SubsystemRow(id) SubsystemRow(variant-code)");
    return \@tuples;
}

=head3 row_to_pegs

    my \@tuples = $shrub->row_to_pegs($row);

Given a subsystem row ID, return a list of the features in the row. For each feature, we include the feature ID and
the feature's role in the subsystem.

=over 4

=item row

The ID of the row whose features are desired.

=item RETURN

Returns a reference to a list of 2-tuples, one per feature, each tuple containing (0) the feature ID,
and (1) the feature's role in the subsystem.

=back

=cut

sub row_to_pegs {
    my($self, $row) = @_;

    my @tuples = $self->GetAll("SubsystemRow Row2Cell SubsystemCell Cell2Feature AND SubsystemCell Cell2Role Role",
                                "SubsystemRow(id) = ?",
                                [$row],
                                "Cell2Feature(to-link) Role(id)");
    return \@tuples;
}

=head3 func_to_pegs

    my $fids = $shrub->func_to_pegs($funcID, $priv);

Return the list of features associated with a specified function ID. The
features returned will be those for which the function is assigned at any
privilege level. Optionally, a specific privilege level can be specified.

=over 4

=item funcID

ID of the function whose features are desired.

=item priv (optional)

If specified, the privilege level at which the function must be assigned.

=item RETURN

Returns a reference to a list of feature IDs for the features to which the function is
assigned.

=back

=cut

sub func_to_pegs {
    # Get the parameters.
    my ($self, $funcID, $priv) = @_;
    # Compute the filter clause.
    my $filter = 'Function2Feature(from-link) = ?';
    # Check for filtering on privilege.
    my @parms = ($funcID);
    if (defined $priv) {
        $filter .= ' AND Function2Feature(security) = ?';
        push @parms, $priv
    }
    # Note we have to filter out duplicates. A single peg may be assigned to a function multiple times
    # if we are not selecting on privilege.
    my %fids = map { $_ => 1 } $self->GetFlat('Function2Feature', $filter, \@parms, 'to-link');
    # Declare the return variable.
    my @retVal = sort keys %fids;
    # Return the result.
    return \@retVal;
}


=head3 get_funcs_and_trans

    my (\%funcs, \%trans) = $shrub->get_funcs_and_trans($g, $security);

Return the functional assignments and protein translations for all the protein-encoding features in a genome.
This essentially calls both L</get_funcs_for_pegs_in_genome> and L</get_trans_for_genome> and returns
the results. The functional assignments returned will be at the highest security level available for each
feature.

=over 4

=item g

ID of the genome whose protein-encoding features are of interest.

=item RETURN

Returns a two-element list. The first element is a reference to a hash mapping each feature ID to a function ID
for its functional assignment. The second element is a reference to a hash mapping each feature ID to its protein
translation. Only protein-encoding features are included in either hash.

=back

=cut

sub get_funcs_and_trans {
    my($self,$g) = @_;

    my $funcsL = $self->get_funcs_for_pegs_in_genome($g,'peg');
    my %funcs  = map { ($_->[0] => $_->[1]) } @$funcsL;

    my $transL = $self->get_trans_for_genome($g);
    my %trans  = map { ($_->[0] => $_->[1]) } @$transL;
    return (\%funcs,\%trans);
}

=head3 get_funcs_for_pegs_in_genome

    my \@tuples = $shrub->get_funcs_for_pegs_in_genome($g, $type);

Return the highest-privilege functional assignments for all features of a given type in a specified genome.

=over 4

=item g

ID of the genome of interest.

=item type

The type of feature desired.

=item RETURN

Returns a reference to a  list of 3-tuples, each consisting of (0) a feature ID, (1) the text of the feature's
functional assignment, and (2) the ID of the function assigned.

=back

=cut

sub get_funcs_for_pegs_in_genome {
    my($self,$g,$type) = @_;

    my @tuples = $self->GetAll("Genome2Feature Feature Feature2Protein Protein AND
                                 Feature Feature2Function Function",
                                "(Genome2Feature(from-link) = ?)
                                 AND (Feature(feature-type) = ?)
                                 ORDER BY Feature2Function(from-link),Feature2Function(security)",
                                [$g,$type],
                                "Genome2Feature(to-link) Function(description) Function(id)");
    my %distinct = map { ($_->[0] => $_) } @tuples;
    @tuples = map { $distinct{$_} } keys(%distinct);  ## This strange code returns the highest securty level
    return \@tuples;
}

=head3 get_trans_for_genome

    my \@tuples = $shrub->get_trans_for_genome($g);

Get all of the protein translations for a genome.

=over 4

=item g

ID of the genome of interest.

=item RETURN

Returns a reference to a list of 2-tuples, each tuple containing (0) a feature ID and (1) the feature's
protein translation.

=back

=cut

sub get_trans_for_genome {
    my($self, $g) = @_;

    my @tuples = $self->GetAll("Genome2Feature Feature Protein",
                                "(Genome2Feature(from-link) = ?) AND (Feature(feature-type) = ?)",
                                [$g,'peg'],
                                "Genome2Feature(to-link) Protein(sequence)");
    return \@tuples;
}

=head3 genome_fasta

    my $fileName = $shrub->genome_fasta($genomeID);

Return the name of the FASTA file for the specified genome.

=over 4

=item genomeID

ID of the genome whose FASTA file is desired.

=item RETURN

Returns the name of the genome's FASTA file, or C<undef> if the genome does not exist in the database.

=back

=cut

sub genome_fasta {
    my ($self, $genomeID) = @_;
    # This will be the return value.
    my $retVal;
    # Get the directory root.
    my $repo = $self->DNArepo;
    # Get the file name.
    my ($contigPath) = $self->GetEntityValues(Genome => $genomeID, 'contig-file');
    if ($contigPath) {
        $retVal = "$repo/$contigPath";
    }
    # Return the name.
    return $retVal;
}

=head3 write_prot_fasta

    $shrub->write_prot_fasta($genome, $oh);

Create a protein FASTA file for the specified genome in the specified output stream.

=over 4

=item genome

ID of the genome whose proteins are desired.

=item oh

Open output file handle or name of the output file. The written output will be a FASTA with the feature ID as the sequence ID
and the protein sequence as the data. Alternatively, a reference to a list. The FASTA triples will be appended to the list.

=back

=cut

sub write_prot_fasta {
    my ($self, $genome, $oh) = @_;
    # Insure we have an open output stream.
    my $ofh;
    if (ref $oh) {
        $ofh = $oh;
    } else {
        open($ofh, '>', $oh) || die "Could not open FASTA output file $oh: $!";
    }
    # Loop through the features.
    my $q = $self->Get('Feature2Protein Protein', 'Feature2Protein(from-link) LIKE ?', ["fig|$genome.peg.%"],
            'Feature2Protein(from-link) Protein(sequence)');
    while (my $record = $q->Fetch()) {
        # Write this feature to the output.
        my ($id, $seq) = $record->Values(['Feature2Protein(from-link)', 'Protein(sequence)']);
        if (ref $ofh eq 'ARRAY') {
            push @$ofh, [$id, '', $seq];
        } else {
            print $ofh ">$id\n$seq\n";
        }
    }
}


=head3 write_peg_fasta

    $shrub->write_peg_fasta($genome, $oh);

Create a DNA FASTA file for the specified genome's protein-encoding genes in the specified output stream.

=over 4

=item genome

ID of the genome whose protein-encoding genes are desired.

=item oh

Open output file handle or name of the output file. The written output will be a FASTA with the feature ID as the sequence ID
and the protein sequence as the data. Alternatively, a reference to a list. The FASTA triples will be appended to the list.

=back

=cut

sub write_peg_fasta {
    my ($self, $genome, $oh) = @_;
    # Insure we have an open output stream.
    my $ofh;
    if (ref $oh) {
        $ofh = $oh;
    }
    if (! ref $oh) {
        open($ofh, '>', $oh) || die "Could not open FASTA output file $oh: $!";
    }
    # Get the genome's contigs.
    require Shrub::Contigs;
    my $contigs = Shrub::Contigs->new($self, $genome);
    # This hash will contain a list of location objects for each feature.
    my %fidLocs;
    # This is the query to loop through the features.
    my $q = $self->Get('Feature2Contig', 'Feature2Contig(from-link) LIKE ? ORDER BY Feature2Contig(from-link), Feature2Contig(ordinal)',
            ["fig|$genome.peg.%"], 'from-link to-link begin dir len');
    # Loop through the feature location data.
    while (my $record = $q->Fetch()) {
        my ($fid, $contig, $begin, $dir, $len) = $record->Values(['from-link', 'to-link', 'begin', 'dir', 'len']);
        push @{$fidLocs{$fid}}, [$contig, $begin, $dir, $len];
    }
    # Now write the feature DNA to the FASTA.
    for my $fid (sort keys %fidLocs) {
        my $locList = $fidLocs{$fid};
        my $dna = $contigs->dna(@$locList);
        if (ref $ofh eq 'ARRAY') {
            push @$ofh, [$fid, '', $dna];
        } else {
            print $ofh ">$fid\n$dna\n";
        }
    }
}


=head2 Query Methods

=head3 DNArepo

    my $dirName = $shrub->DNArepo($optional);

Returns the name of the directory containing the DNA repository.

=over 4

=item optional

If TRUE, then a null value is returned if there is no DNA repository. If FALSE (the default), an error
occurs in this case.

=back

=cut

sub DNArepo {
    my ($self, $optional) = @_;
    my $retVal = $self->{dnaRepo};
    if (! $retVal && ! $optional) {
        die "DNA is not supported in this version of SEEDtk.";
    }
    return $self->{dnaRepo};
}


=head3 ProteinID

    my $key = $shrub->ProteinID($sequence);

or

    my $key = Shrub::ProteinID($sequence);

Return the protein sequence ID that would be associated with a specific
protein sequence.

=over 4

=item sequence

String containing the protein sequence in question.

=item RETURN

Returns the ID value for the specified protein sequence. If the sequence exists
in the database, it will have this ID in the B<Protein> table.

=back

=cut

sub ProteinID {
    # Convert from the instance form of the call to a direct call.
    shift if UNIVERSAL::isa($_[0], __PACKAGE__);
    # Get the parameters.
    my ($sequence) = @_;
    # Compute the MD5 hash.
    my $retVal = Digest::MD5::md5_hex($sequence);
    # Return the result.
    return $retVal;
}


=head3 FormatRole

    my $roleText = Shrub::FormatRole($ecNum, $tcNum, $description);

Format the text of a role given its EC, TC, and description information.

=over 4

=item ecNum

EC number of the role, or an empty string if there is no EC number.

=item tcNum

TC number of the role, or an empty string if there is no TC number.

=item description

Descriptive text of the role.

=item RETURN

Returns the full display text of the role.

=back

=cut

sub FormatRole {
    return ($_[2] . ($_[0] ? " (EC $_[0])" : '') . ($_[1] ? " (TC $_[1])" : ''));
}


=head2 Public Constants

=head3 Privilege Level Constants

The following constants indicate privilege levels.

=over 4

=item PUBLIC

Lowest privilege (0), indicating a publically assigned annotation.

=item PROJ

Middle privilege (1), indicating a projected annotation.

=item PRIV

Highest privilege (2), indicating a core-curated annotation.

=back

=cut

    use constant PUBLIC => 0;
    use constant PROJ => 1;
    use constant PRIV => 2;

=head3 MAX_PRIVILEGE

    my $priv = Shrub::MAX_PRIVILEGE;

Return the maximum privilege level for functional assignments.

=cut

    use constant MAX_PRIVILEGE => 2;

=head2 Function and Role Utilities

=head3 Checksum

    my $checksum = Shrub::Checksum($text);

or

    my $checksum = $shrub->Checksum($text);

Compute the checksum for a text string. This is currently a simple MD5 digest.

=over 4

=item text

Text string to digest.

=item RETURN

Returns a fixed-length, digested form of the string.

=back

=cut

sub Checksum {
    # Convert from the instance form of the call to a direct call.
    shift if UNIVERSAL::isa($_[0], __PACKAGE__);
    # Return the digested string.
    my ($text) = @_;
    my $retVal = Digest::MD5::md5_base64($text);
    return $retVal;
}


=head3 roles_of_func

    my @roles = Shrub::roles_of_func($funcID);

Return the role IDs represented in a function ID.

=over 4

=item funcID

The function ID to parse.

=item RETURN

Returns a list of the roles in the function (usually a single role).

=back

=cut

sub roles_of_func {
    my ($funcID) = @_;
    my @retVal = split /[;\/@]/, $funcID;
    return @retVal;
}


=head2 Virtual Methods

=head3 PreferredName

    my $name = $erdb->PreferredName();

Return the variable name to use for this database when generating code.

=cut

sub PreferredName {
    return 'shrub';
}


1;

