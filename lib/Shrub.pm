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
    use ERDBGenerate;
    use XML::Simple;
    use Digest::MD5;
    use Getopt::Long::Descriptive;

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

=head3 Global Section Constant

Each section of the database used by the loader corresponds to a single genome.
The global section is loaded after all the others, and is concerned with data
not related to a particular genome.

=cut

    # Name of the global section
    use constant GLOBAL => 'Globals';

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

    my ($shrub, $opt, $usage) = Shrub->new_for_script($program, \%tuning, @options);

Construct a new Shrub object for a command-line script. This method
uses a call to L<GetOpt::Long::Descriptive/describe_options> to parse the command-line
options, passing in the incoming B<program> and B<options> parameters.

If the command-line parse fails, the method will die with a usage message.

For example

    my ($shrub, $opt, $usage) = Shrub->new_for_script('%c %o genome1 genome2 ...',
            { offline => 1},
            [ 'missing|m', "only load missing genomes"],
            [ 'directory|d=s', "source directory", { default => '/vol/fig/shrub_data' }]);

will create an offline L<Shrub> object. The C<$opt> object will have a member
C<missing> that is TRUE if we should only load missing genomes and a member
C<directory> that returns the source directory. The C<text> member of C<$usage> can
be used to display a usage string in error messages, and if the user invokes the
C<--help> option on the command line, all of the option information will be displayed
in full detail.

The following command-line options (all of which are optional) will
be processed by this method automatically and used to construct the Shrub object.

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

The B<tuning> parameter is a reference to a hash with the following members.

=over 4

=item externalDBD

If TRUE, use of an external DBD will be forced, overriding the DBD stored in the database.

=item offline

If TRUE, the database object will be constructed but not connected to the database.

=back

=cut

sub new_for_script {
    # Get the parameters.
    my ($class, $program, $tuning, @options) = @_;
    # Parse the command line.
    my ($opt, $usage) = describe_options($program,
           [ "loadDirectory=s", "directory for creating table load files" ],
           [ "DBD=s", "file containing the database definition XML" ],
           [ "dbName=s", "database name" ],
           [ "sock=s", "MYSQL socket" ],
           [ "userData=s", "name/password for database logon" ],
           [ "dbhost=s", "database host server" ],
           [ "port=i", "mysql port" ],
           [ "dbms=s", "database management system" ],
           [ "repository=s", "genome repository directory root" ],
           [ "help|h", "display usage information", { shortcircuit => 1}],
           @options);
    # The above method dies if the options are invalid. Check here for the HELP option.
    if ($opt->help) {
        print $usage->text;
        exit;
    }
    # Check for an external DBD override.
    my $externalDBD = $tuning->{externalDBD} || $opt->dbd;
    # Here we have a real invocation, so we can create the Shrub object.
    my $retVal = Shrub::new($class, loadDirectory => $opt->loaddirectory, DBD => $opt->dbd,
            dbName => $opt->dbname, sock => $opt->sock, userData => $opt->userdata,
            dbhost => $opt->dbhost, port => $opt->port, dbms => $opt->dbms,
            repository => $opt->repository, offline => $tuning->{offline},
            externalDBD => $externalDBD
            );
    # Return the result.
    return ($retVal, $opt, $usage);
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

=head3 SubsystemID

    my $subID = $shrub->SubsystemID($subName);

or

    my $subID = Shrub::SubsystemID($subName);

Return the ID of the subsystem with the specified name.

=over 4

=item subName

Name of the relevant subsystem. A subsystem name with underscores for spaces
will return the same ID as a subsystem name with the spaces still in it.

=item RETURN

Returns a normalized subsystem name.

=back

=cut

sub SubsystemID {
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


=head2 Public Constants

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
    # Separate out the comment (if any).
    my $statement = $function;
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


=head2 Configuration Methods

=head3 GlobalSection

    my $flag = $shrub->GlobalSection($name);

Return TRUE if the specified section name is the global section, FALSE
otherwise.

=over 4

=item name

Section name to test.

=item RETURN

Returns TRUE if the parameter matches the GLOBAL constant, else FALSE.

=back

=cut

sub GlobalSection {
    # Get the parameters.
    my ($self, $name) = @_;
    # Return the result.
    return ($name eq GLOBAL);
}


=head2 Virtual Methods

=head3 PreferredName

    my $name = $erdb->PreferredName();

Return the variable name to use for this database when generating code.

=cut

sub PreferredName {
    return 'shrub';
}

=head3 GetSourceObject

    my $source = $erdb->GetSourceObject();

Return the object to be used in creating load files for this database. The Shrub
does not have a source object, so we return nothing.

=cut

sub GetSourceObject {
    my ($self) = @_;
    return undef;
}

=head3 SectionList

    my @sections = $erdb->SectionList();

Return a list of the names for the different data sections used when loading this database.
The default is a single string, in which case there is only one section representing the
entire database.

=cut

sub SectionList {
    # Get the parameters.
    my ($self) = @_;
    # The section names will be put in here.
    my @retVal;
    # Get the name of the section control file.
    my $controlFileName = ERDBGenerate::CreateFileName("SectionList", undef, 'control', $self->LoadDirectory());
    # Check to see if it exists.
    if (-f $controlFileName) {
        # Yes. Pull out the sections from it.
        Trace("Reading section list from $controlFileName.") if T(ERDBGenerate => 2);
        @retVal = Tracer::GetFile($controlFileName);
    } else {
        # No, so we have to create it. Get the genome repository's directories.
        my @genomes = grep { $_ =~ /\d+\.\d+/ } Tracer::OpenDir($self->{repository});
        @retVal = sort @genomes;
        # Append the global section.
        push @retVal, GLOBAL;
        # Write out the control file with the new sections.
        Trace("Writing section list to $controlFileName.") if T(ERDBGenerate => 2);
        Tracer::PutFile($controlFileName, \@retVal);
    }
    # Return the section list.
    return @retVal;
}

=head3 Loader

    my $groupLoader = $erdb->Loader($groupName, $source, $options);

Return an L<ERDBLoadGroup> object for the specified load group. This method is used
by L<ERDBGenerator.pl> to create the load group objects. If you are not using
L<ERDBGenerator.pl>, you don't need to override this method.

=over 4

=item groupName

Name of the load group whose object is to be returned. The group name is
guaranteed to be a single word with only the first letter capitalized.

=item source

The source object used to access the data from which the load file is derived. This
is the same object returned by L</GetSourceObject>; however, we allow the caller to pass
it in as a parameter so that we don't end up creating multiple copies of a potentially
expensive data structure. It is permissible for this value to be undefined, in which
case the source will be retrieved the first time the client asks for it.

=item options

Reference to a hash of command-line options.

=item RETURN

Returns an L<ERDBLoadGroup> object that can be used to process the specified load group
for this database.

=back

=cut

sub Loader {
    # Get the parameters.
    my ($self, $groupName, $options) = @_;
    # Compute the loader name.
    my $loaderClass = "${groupName}ShrubLoader";
    # Pull in its definition.
    require "$loaderClass.pm";
    # Create an object for it.
    my $retVal = eval("$loaderClass->new(\$self, \$options)");
    # Insure it worked.
    Confess("Could not create $loaderClass object: $@") if $@;
    # Return it to the caller.
    return $retVal;
}

=head3 LoadGroupList

    my @groups = $erdb->LoadGroupList();

Returns a list of the names for this database's load groups. This method is used
by L<ERDBGenerator.pl> when the user wishes to load all table groups. The default
is a single group called 'All' that loads everything.

=cut

sub LoadGroupList {
    # Return the list.
    return qw(Genome Subsystem Taxonomy Chemistry);
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
