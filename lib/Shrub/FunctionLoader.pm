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

package Shrub::FunctionLoader;

    use strict;
    use Shrub::DBLoader;
    use Shrub;
    use SeedUtils;
    use BasicLocation;

=head1 Shrub Function/Role Loader

This package is used to load functions and roles. During initialization, the functions and roles
currently in the database are read into a memory hash. New functions are checked to see if they
are already in the database, and if they are not, they are inserted and connected to the
appropriate roles.

This object has the following fields.

=over 4

=item loader

L<Shrub::DBLoader> object used to access the database and the hash tables.

=item roleHash

A hash mapping role MD5s to role IDs. This is used to find out if roles are already in
the database.

=item funHash

A hash mapping function MD5s to role IDs. This is used to find out if functions are
already in the database.

=back

=head2 ** IMPORTANT NOTE **

This object will not work unless no other process is modifying the function and role data in the
database. At the current time, we have no way to enforce this.

=head2 Special Methods

=head3 new

    my $funcLoader = Shrub::FunctionLoader->new($loader, %options);

Construct a new Shrub function loader object and initialize the hash tables.

=over 4

=item loader

L<Shrub::DBLoader> object to be used to access the database and the load utility methods.

=item options

A hash of options relating to this object. The following keys are supported.

=over 8

=item rolesOnly

If TRUE, functions will not be processed. The function hash will not be
created and the function tables will not be opened for loading. The default
is FALSE.

=item slow

If TRUE, tables will be loaded with individual inserts instead of file loading
when the L<Shrub::DBLoader> object is closed.

=back

=back

=cut

sub new {
    # Get the parameters.
    my ($class, $loader, %options) = @_;
    # Create the object.
    my $retVal = { loader => $loader };
    # Determine if this is slow mode.
    my $slowMode = $options{slow};
    # Prepare to load the role database table.
    if (! $slowMode) {
        $loader->Open('Role');
    }
    # Are we processing functions?
    if (! $options{rolesOnly}) {
        # Yes. Load the function table into memory.
        my $funHash = $loader->CreateTableHash('Function', 'checksum');
        $retVal->{funHash} = $funHash;
        # Prepare to load the function-related database tables.
        if (! $slowMode) {
            # This causes inserts to be spooled into files
            # for loading when the loader object is closed.
            $loader->Open(qw(Function Function2Role));
        }
    }
    # Load the role table into memory.
    my $roleHash = $loader->CreateTableHash('Role', 'checksum');
    # Save the role hash.
    $retVal->{roleHash} = $roleHash;
    # Bless and return the object.
    bless $retVal, $class;
    return $retVal;
}

=head2 Public Manipulation Methods

=head3 ProcessFunction

    my $fun_id = $funcLoader->ProcessFunction($checksum, $statement, $sep, \%roles, $comment);

Get the ID of a functional assignment. The function is inserted into the database and connected to its
constituent roles if it does not already exist. The function must already have been parsed by
L<Shrub/ParseFunction>.

=over 4

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

=item RETURN

Returns the ID code associated with the functional assignment.

=back

=cut

sub ProcessFunction {
    # Get the parameters.
    my ($self, $checksum, $statement, $sep, $roles, $comment) = @_;
    # Get the loader object.
    my $loader = $self->{loader};
    # Get the statistics object.
    my $stats = $loader->stats;
    # Is this function already in the database?
    my $funHash = $self->{funHash};
    my $retVal = $funHash->{$checksum};
    if (! $retVal) {
        # No, we must insert it. Get an ID for it.
        $retVal = $loader->db->NewID();
        # Put in the roles first.
        for my $role (keys %$roles) {
            # Get this role's checksum.
            my $roleCheck = $roles->{$role};
            # Get the role's ID.
            my ($roleID) = $self->ProcessRole($role, $roleCheck);
            # Connect the role to the function.
            $loader->InsertObject('Function2Role', 'from-link' => $retVal, 'to-link' => $roleID);
            $stats->Add(function2role => 1);
        }
        # Now put in the function itself.
        $loader->InsertObject('Function', id => $retVal, checksum => $checksum, sep => $sep,
            description => $statement);
        # Save its ID for next time.
        $funHash->{$checksum} = $retVal;
    }
    # Return the function ID.
    return $retVal;
}

=head3 ProcessRole

    my ($roleID, $roleMD5) = $funcLoader->ProcessRole($role, $checksum);

Return the ID of a role in the database. If the role does not exist, it will be inserted.

=over 4

=item role

Text of the role to find.

=item checksum (optional)

If the checksum of the role is already known, it can be passed in here.

=item RETURN

Returns a two-element list containing (0) the ID of the role in the database and (1) the
role's MD5 checksum.

=back

=cut

sub ProcessRole {
    # Get the parameters.
    my ($self, $role) = @_;
    # Get the loader object.
    my $loader = $self->{loader};
    # Parse the role components.
    my ($roleText, $ecNum, $tcNum, $hypo) = Shrub::ParseRole($role);
    # Compute the checksum.
    my $roleNorm = Shrub::RoleNormalize($role);
    my $checkSum = Shrub::Checksum($roleNorm);
    # Get the role ID hashes.
    my $roleHash = $self->{roleHash};
    my $suffixHash = $self->{roleSuffixes};
    # Do we already have this role?
    my $retVal = $roleHash->{$checkSum};
    if (! $retVal) {
        # No. Get an ID.
        $retVal = $loader->db->NewID();
        # Insert the role.
        $loader->InsertObject('Role', id => $retVal, checksum => $checkSum, 'ec-number' => $ecNum,
                'tc-number' => $tcNum, hypo => $hypo, description => $roleText,
                'proposed-description' => $roleText);
        # Save its ID in the hash.
        $roleHash->{$checkSum} = $retVal;
    }
    # Return the role information.
    return ($retVal, $checkSum);
}


=head3 ReadProteins

    my $protHash = $funcLoader->ReadProteins($genome, $genomeDir);

Connect the proteins found in the specified genome's peg translation file to the functions in the
specified hash. It can also optionally connect the peg Feature records to the translations themselves.

=over 4

=item genome

ID of the genome whose protein file is to be read.

=item genomeDir

Directory containing the genome source files.

=item RETURN

Returns a reference to a hash mapping feature iDs to protein IDs. The proteins will have been
inserted into the database.

=back

=cut

sub ReadProteins {
    # Get the parameters.
    my ($self, $genome, $genomeDir) = @_;
    # Get the loader object.
    my $loader = $self->{loader};
    # Get the statistics object.
    my $stats = $loader->stats;
    # The return hash will go in here.
    my %retVal;
    # Open the genome's protein FASTA.
    print "Reading $genome proteins.\n";
    my $fh = $loader->OpenFasta(protein => "$genomeDir/peg-trans");
    # Loop through the proteins.
    while (my $protDatum = $loader->GetLine(protein => $fh)) {
        my ($pegId, undef, $seq) = @$protDatum;
        # Compute the protein ID.
        my $protID = Shrub::ProteinID($seq);
        # Insert the protein into the database.
        $loader->InsertObject('Protein', id => $protID, sequence => $seq);
        # Connect the protein to the feature in the hash.
        $retVal{$pegId} = $protID;
    }
    # Return the protein hash.
    return \%retVal;
}


=head3 ReadFeatures

    $funcLoader->ReadFeatures($genome, $fileName, $priv, \%protHash);

Read the feature information from a tab-delimited feature file. For each feature, the file contains
the feature ID, its location string (Sapling format), and its functional assignment. This method
will insert the feature, connect it to the genome and the contig, then attach the functional
assignment.

=over 4

=item genome

ID of the genome whose feature file is being processed.

=item fileName

Name of the file containing the feature data to process.

=item priv

Privilege level for the functional assignments.

=item protHash (optional)

A hash mapping feature IDs to protein IDs.

=back

=cut

sub ReadFeatures {
    # Get the parameters.
    my ($self, $genome, $fileName, $priv, $protHash) = @_;
    # Get the loader object.
    my $loader = $self->{loader};
    # If no protein hash was provided, create an empty one.
    $protHash //= {};
    # Get the statistics object.
    my $stats = $loader->stats;
    # Open the file for input.
    my $ih = $loader->OpenFile(feature => $fileName);
    # Loop through the feature file.
    while (my $featureDatum = $loader->GetLine(feature => $ih)) {
        # Get the feature elements.
        my ($fid, $locString, $function) = @$featureDatum;
        # Create a list of location objects from the location string.
        my @locs = map { BasicLocation->new($_) } split /\s*,\s*/, $locString;
        $stats->Add(featureLocs => scalar(@locs));
        # Compute the feature type.
        my $ftype;
        if ($fid =~ /fig\|\d+\.\d+\.(\w+)\.\d+/) {
            $ftype = $1;
        } else {
            die "Invalid feature ID $fid.";
        }
        # If this is NOT a peg and has no function, change the function to
        # 'unspecified'. Otherwise it will be converted to
        # "hypothetical protein".
        if ($ftype ne 'peg' && ! $function) {
            $function = "unspecified $ftype";
        }
        # Compute the total sequence length.
        my $seqLen = 0;
        for my $loc (@locs) {
            $seqLen += $loc->Length;
        }
        # Compute the protein.
        my $protID = $protHash->{$fid} // '';
        # Connect the feature to the genome.
        $loader->InsertObject('Feature', id => $fid, 'feature-type' => $ftype,
                'sequence-length' => $seqLen, Genome2Feature_link => $genome,
                Protein2Feature_link => $protID);
        $stats->Add(feature => 1);
        # Connect the feature to the contigs. This is where the location information figures in.
        my $ordinal = 0;
        for my $loc (@locs) {
            $loader->InsertObject('Feature2Contig', 'from-link' => $fid, 'to-link' => ($genome . ":" . $loc->Contig),
                    begin => $loc->Left, dir => $loc->Dir, len => $loc->Length, ordinal => ++$ordinal);
            $stats->Add(featureSegment => 1);
        }
        # Parse the function.
        my @parsed = Shrub::ParseFunction($function);
        # Insure it is in the database.
        my $funcID = $self->ProcessFunction(@parsed);
        # Get the comment.
        my $comment = $parsed[4];
        # Connect the functions.
         # Make the connection at each privilege level.
         for (my $p = $priv; $p >= 0; $p--) {
             $loader->InsertObject('Feature2Function', 'from-link' => $fid, 'to-link' => $funcID,
                     comment => $comment, security => $p);
             $stats->Add(featureFunction => 1);
         }
    }
}

1;