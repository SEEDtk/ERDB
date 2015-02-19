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
    use Tracer;
    use base qw(ERDB);
    use Stats;
    use DBKernel;
    use SeedUtils;
    use Digest::MD5;


=head1 Shrub Database Package

=head2 Introduction

The Shrub database is a new Entity-Relationship Database that implements
the repository for the SEEDtk system. This object has minimal
capabilities: most of its power comes the L<ERDB> base class.

The fields in this object are as follows.

=over 4

=item loadDirectory

Name of the directory containing the files used by the loaders.

=item repository

Name of the directory containing the genome repository.

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

=item repository

Name of the directory containing the genome repository.

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
    my $dirBase = $FIG_Config::shrub_dir || '/vol/seedtk/shrub';
    # Get the options.
    if (! $options{loadDirectory}) {
        $options{loadDirectory} = "$dirBase/LoadFiles";
    }
    my $dbd = $options{DBD} || $FIG_Config::shrub_dbd || "$dirBase/ShrubDBD.xml";
    my $dbName = $options{dbName} || $FIG_Config::shrubDB || "seedtk_shrub";
    my $userData = $options{userData} || $FIG_Config::userData || "seed/";
    my $dbhost = $options{dbhost} || $FIG_Config::dbhost || "seed-db-write.mcs.anl.gov";
    my $repository = $options{repository} || "$FIG_Config::shrub_dna";
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
    Trace("Connecting to shrub database.") if T(2);
    # Connect to the database, if desired.
    my $dbh;
    if (! $options{offline}) {
         $dbh = DBKernel->new($dbms, $dbName, $user, $pass, $port, $dbhost, $sock);
    }
    # Create the ERDB object.
    my $retVal = ERDB::new($class, $dbh, $dbd, %options);
    # Attach the repository pointer.
    $retVal->{repository} = $repository;
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
            repository => $opt->repository, offline => $tuning{offline},
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

=item repository

Name of the directory containing the genome repository.

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
           [ "repository=s", "genome repository directory root" ],
    );
}


=head2 Public Methods

=head3 DNArepo

    my $dirName = $shrub->DNArepo

Returns the name of the directory containing the DNA repository.

=cut

sub DNArepo {
    my ($self) = @_;
    return $self->{repository};
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

=head3 NormalizedName

    my $subID = $shrub->NormalizedName($subName);

or

    my $subID = Shrub::NormalizedName($subName);

Return the normalized name of the subsystem with the specified name. A subsystem
name with underscores for spaces will return the same normalized name as a subsystem
name with the spaces still in it.

=over 4

=item subName

Name of the relevant subsystem.

=item RETURN

Returns a normalized subsystem name.

=back

=cut

sub NormalizedName {
    # Convert from the instance form of the call to a direct call.
    shift if UNIVERSAL::isa($_[0], __PACKAGE__);
    # Get the parameters.
    my ($subName) = @_;
    # Normalize the subsystem name by converting underscores to spaces.
    # Underscores at the beginning and end are not converted.
    my $retVal = $subName;
    my $trailer = chop $retVal;
    my $prefix = substr($retVal,0,1);
    $retVal = substr($retVal, 1);
    $retVal =~ tr/_/ /;
    $retVal = $prefix . $retVal . $trailer;
    # Return the result.
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
(0) the function ID, (1) the function statement, and (2) the associated comment.

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
        # Is this a peg?
        if ($feature =~ /peg/) {
            # Yes. Get the function via the protein.
            ($functionData) = $self->GetAll('Feature2Protein Protein Protein2Function Function',
                    'Feature2Protein(from-link) = ? AND Protein2Function(security) = ?',
                    [$feature, $priv], 'Function(id) Function(description) Protein2Function(comment)');
        } else {
            # No. Get the function directly.
            ($functionData) = $self->GetAll('Feature2Function Function',
                    'Feature2Function(from-link) = ? AND Feature2Function(security) = ?',
                    [$feature, $priv], 'Function(id) Function(description) Feature2Function(comment)');
        }
        # Store the function data in the return hash.
        $retVal{$feature} = $functionData;
    }
    # Return the computed hash.
    return \%retVal;
}

=head3 Subsystem2Feature

    my $fidList = $shrub->Subsystem2Feature($fld => $sub);

Return a list of all the features in a single subsystem. The subsystem can be
identified by name or ID. So, for example

    my $fidList = $shrub->Subsystem2Feature(name => 'Histidine Degradation');

would process the subsystem named C<Histidine Degradation>, while

    my $fidList = $shrub->Subsystem2Feature(id => 'HD1);

would process the subsystem with ID C<HD1>.

=over 4

=item fld

Field being used to identify the subsystem-- C<name> for the subsystem name,
or C<id> for the subsystem ID.

=item sub

The name or ID of the subsystem of interest.

=item RETURN

Returns a reference to a list of the feature IDs for al the features that have
been populated in the subsystem.

=back

=cut

sub Subsystem2Feature {
    # Get the parameters.
    my ($self, $fld, $sub) = @_;
    # Read the subsystem features from the database. NOTE that right now the ID and name
    # are the same field.
    my @retVal = $self->GetFlat('Subsystem Subsystem2Feature', "Subsystem(id) = ?", [$sub],
            'Subsystem2Feature(to-link)');
    # Return the result.
    return \@retVal;
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

=head3 EC_PATTERN

    $string =~ /$Shrub::EC_PATTERN/;

Pre-compiled pattern for matching EC numbers.

=cut

    our $EC_PATTERN = qr/\(\s*E\.?C\.?(?:\s+|:)(\d\.(?:\d+|-)\.(?:\d+|-)\.(?:n?\d+|-)\s*)\)/;

=head3 TC_PATTERN

    $string =~ /$Shrub::TC_PATTERN/;

Pre-compiled pattern for matchin TC numbers.

=cut

    our $TC_PATTERN = qr/\(\s*T\.?C\.?(?:\s+|:)(\d\.[A-Z]\.(?:\d+|-)\.(?:\d+|-)\.(?:\d+|-)\s*)\)/;

=head2 Function and Role Utilities

=head3 RoleNormalize

    my $normalRole = Shrub::RoleNormalize($role);

or

    my $normalRole = $shrub->RoleNormalize($role);

Normalize a role by removing extra spaces, stripping off the EC number, and converting it to lower case.

=over 4

=item role

Role text to normalize.

=item RETURN

Returns a normalized form of the role.

=back

=cut

sub RoleNormalize {
    # Convert from the instance form of the call to a direct call.
    shift if UNIVERSAL::isa($_[0], __PACKAGE__);
    # Get the parameters.
    my ($role) = @_;
    # Remove the EC number.
    $role =~ s/$EC_PATTERN//;
    # Remove the TC identifier.
    $role =~ s/$TC_PATTERN//;
    # Remove the extra spaces.
    $role =~ s/\s+/ /g;
    $role =~ s/^\s+//;
    $role =~ s/\s+$//;
    # Convert to lower case.
    my $retVal = lc $role;
    # Return the result.
    return $retVal;
}

=head3 ParseRole

    my ($roleText, $ecNum, $tcNum, $hypo) = $shrub->ParseRole($role);

or

    my ($roleText, $ecNum, $tcNum, $hypo) = Shrub::ParseRole($role);

Parse a role. The EC and TC numbers are extracted and an attempt is made to determine if the role is
hypothetical.

=over 4

=item role

Text of the role to parse.

=item RETURN

Returns a four-element list consisting of the main role text, the EC number (if any),
the TC number (if any), and a flag that is TRUE if the role is hypothetical and FALSE
otherwise.

=back

=cut

sub ParseRole {
    # Convert from the instance form of the call to a direct call.
    shift if UNIVERSAL::isa($_[0], __PACKAGE__);
    # Get the parameters.
    my ($role) = @_;
    # Extract the EC number.
    my ($ecNum, $tcNum) = ("", "");
    my $roleText = $role;
    if ($role =~ /(.+?)\s*$EC_PATTERN\s*(.*)/) {
        $roleText = $1 . $3;
        $ecNum = $2;
    } elsif ($role =~ /(.+?)\s*$TC_PATTERN\s*(.*)/) {
        $roleText = $1 . $3;
        $tcNum = $2;
    }
    # Check for a hypothetical.
    my $hypo = SeedUtils::hypo($roleText);
    # Return the parse results.
    return ($roleText, $ecNum, $tcNum, $hypo);
}

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
    return Digest::MD5::md5_base64($_[0]);
}

=head3 ParseFunction

    my ($checksum, $statement, $sep, \%roles, $comment) = $shrub->ParseFunction($function);

or

    my ($checksum, $statement, $sep, \%roles, $comment) = Shrub::ParseFunction($function);

Parse a functional assignment. This method breaks it into its constituent roles,
pulls out the comment and the separator character, and computes the checksum.

=over 4

=item function

Functional assignment to parse.

=item RETURN

Returns a five-element list containing the following.

=over 8

=item checksum

The unique checksum for this function. Any function with the same roles and the same
separator will have the same checksum.

=item statement

The text of the function with the EC numbers and comments removed.

=item sep

The separator character. For a single-role function, this is always C<@>. For multi-role
functions, it could also be C</> or C<;>.

=item roles

Reference to a hash mapping each constituent role to its checksum.

=item comment

The comment string containing in the function. If there is no comment, will be an empty
string.

=back

=back

=cut

sub ParseFunction {
    # Convert from the instance form of the call to a direct call.
    shift if UNIVERSAL::isa($_[0], __PACKAGE__);
    # Get the parameters.
    my ($function) = @_;
    # Separate out the comment (if any). Note we convert an undefined function
    # to an empty string.
    my $statement = $function // "";
    my $comment = "";
    if ($function && $function =~ /(.+?)\s*[#!](.+)/) {
        ($statement, $comment) = ($1, $2);
    }
    # The roles and the separator will go in here.
    my @roles;
    my $sep = '@';
    # This will be the role hash.
    my %roles;
    # This will contain the checksum.
    my $checksum;
    # Check for suspicious elements.
    my $malformed;
    if (! $statement || $statement eq 'hypothetical protein') {
        # Here we have a hypothetical protein. This is considered well-formed but without
        # any roles.
    } elsif ($function =~ /\b(?:similarit|blast\b|fasta|identity)|%|E=/i) {
        # Here we have suspicious elements.
        $malformed = 1;
    } else {
        # Parse out the roles.
        my @roleParts = split(/\s*(\s\@|\s\/|;)\s+/, $statement);
        # Check for a role that is too long.
        if (grep { length($_) > 250 } @roles) {
            $malformed = 1;
        } elsif (scalar(@roleParts) == 1) {
            # Here we have the normal case, a single-role function.
            @roles = @roleParts;
        } else {
            # With multiple roles, we need to extract the separator and peel out the
            # roles.
            $sep = substr($roleParts[1], -1);
            for (my $i = 0; $i < scalar(@roleParts); $i += 2) {
                push @roles, $roleParts[$i];
            }
        }
    }
    # If we are malformed, there are no roles, but we checksum the function.
    if ($malformed) {
        $checksum = Checksum($function);
    } else {
        # Here we have to compute a checksum from the roles and the separator.
        my @normalRoles = map { RoleNormalize($_) } @roles;
        $checksum = Checksum($sep . join("\t", @normalRoles));
        # Now create the role hash.
        for (my $i = 0; $i < scalar(@roles); $i++) {
            $roles{$roles[$i]} = Checksum($normalRoles[$i]);
        }
    }
    # Return the parsed function data.
    return ($checksum, $statement, $sep, \%roles, $comment);
}


=head2 Virtual Methods

=head3 PreferredName

    my $name = $erdb->PreferredName();

Return the variable name to use for this database when generating code.

=cut

sub PreferredName {
    return 'shrub';
}

=head3 UseInternalDBD

    my $flag = $erdb->UseInternalDBD();

Return TRUE if this database should be allowed to use an internal DBD.
The internal DBD is stored in the C<_metadata> table, which is created
when the database is loaded. The Shrub uses an internal DBD.

=cut

sub UseInternalDBD {
    return 1;
}

1;
