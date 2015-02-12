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

package ShrubFunctionLoader;

    use strict;
    use ShrubLoader;
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

L<ShrubLoader> object used to access the database and the hash tables.

=back

=head2 Special Methods

=head3 new

    my $funcLoader = ShrubFunctionLoader->new($loader);

Construct a new Shrub function loader object and initialize the hash tables.

=over 4

=item loader

L<ShrubLoader> object to be used to access the database and the load utility methods.

=back

=cut

sub new {
    # Get the parameters.
    my ($class, $loader) = @_;
    # Create the object.
    my $retVal = { loader => $loader };
    # Load the function and role tables into memory.
    $loader->CreateTableHash('Function', 'checksum');
    $loader->CreateTableHash('Role', 'checksum');
    # Bless and return the object.
    bless $retVal, $class;
    return $retVal;
}

=head2 Public Manipulation Methods

=head3 ProcessFunction

    my ($fid, $comment) = $funcLoader->ProcessFunction($function);

Get the ID of a functional assignment. The function is inserted into the database and connected to its
constituent roles if it does not already exist.

=over 4

=item function

Functional assignment to insert into the database.

=item RETURN

Returns a two-element list consisting of the ID code associated with the functional assignment and the
comment (if any) extracted from it.

=back

=cut

sub ProcessFunction {
    # Get the parameters.
    my ($self, $function) = @_;
    # Get the loader object.
    my $loader = $self->{loader};
    # Get the database object.
    my $shrub = $loader->db;
    # Get the statistics object.
    my $stats = $loader->stats;
    # Parse the function to get its roles and its checksum.
    my ($checksum, $statement, $sep, $roles, $comment) = Shrub::ParseFunction($function);
    # Get the function's ID. This may insert the function into the database.
    my ($retVal, $newFlag) = $loader->InsureTable('Function', checksum => $checksum, sep => $sep,
            statement => $statement);
    # If we inserted the function, we need to connect the roles. Note that a hypothetical protein or
    # malformed function will have no roles.
    if ($newFlag) {
        # Loop through the roles.
        for my $role (keys %$roles) {
            # Get this role's checksum.
            my $roleCheck = $roles->{$role};
            # Parse the role components.
            my ($roleText, $ecNum, $tcNum, $hypo) = Shrub::ParseRole($role);
            # Get the role's ID.
            my ($roleID) = $loader->InsureTable('Role', checksum => $roleCheck, 'ec-number' => $ecNum,
                    'tc-number' => $tcNum, hypo => $hypo, statement => $roleText);
            # Connect the role to the function.
            $shrub->InsertObject('Function2Role', 'from-link' => $retVal, 'to-link' => $roleID);
            $stats->Add(function2role => 1);
        }
    }
    # Return the function ID and the comment string.
    return ($retVal, $comment);
}

=head3 ProcessRole

    my $roleID = $funcLoader->ProcessRole($role);

Return the ID of a role in the database. If the role does not exist, it will be inserted.

=over 4

=item role

Text of the role to find.

=item RETURN

Returns the ID of the role in the database.

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
    # Get the role's ID. This will insert the role if it is not already present.
    my ($retVal) = $loader->InsureTable('Role', checksum => $checkSum, 'ec-number' => $ecNum,
            'tc-number' => $tcNum, hypo => $hypo, statement => $roleText);
    # Return it.
    return $retVal;
}


=head3 ConnectPegFunctions

    $funcLoader->ConnectPegFunctions($genome, $genomeDir, $priv, \%gPegHash, %options);

Connect the proteins found in the specified genome's peg translation file to the functions in the
specified hash. It can also optionally connect the peg Feature records to the translations themselves.

=over 4

=item genome

ID of the genome whose protein file is to be read.

=item genomeDir

Directory containing the genome source files.

=item gPegHash

Reference to a hash mapping peg IDs to function assignments. Only the pegs in the hash will be
processed.

=item options

Hash containing options modifying the process. The keys of interest are as follows.

=over 8

=item translationLinks

If C<0>, then the pegs will not be linked to the protein translations. If C<1>, then the pegs will
be linked to the protein translations. The default is C<1>.

=item priv

Privilege level for the function assignments. Assignments will be attached at this privilege
level and all levels below it. The default is C<0>.

=back

=back

=cut

sub ConnectPegFunctions {
    # Get the parameters.
    my ($self, $genome, $genomeDir, $gPegHash, %options) = @_;
    # Determine if we are translating links. Note the use of the // operator: if the value is underfined,
    # it defaults to 1.
    my $translateLinks = $options{translateLinks} // 1;
    # Determine the privilege level. The default is 0.
    my $priv = $options{priv} // 0;
    # Get the loader object.
    my $loader = $self->{loader};
    # Get the statistics object.
    my $stats = $loader->stats;
    # Open the genome's protein FASTA.
    print "Processing $genome peg functions.\n";
    my $fh = $loader->OpenFasta("$genomeDir/peg-trans", 'protein');
    # Loop through the proteins.
    while (my $protDatum = $loader->GetLine($fh, 'protein')) {
        my ($pegId, undef, $seq) = @$protDatum;
        my $function = $gPegHash->{$pegId};
        # Are we interested in this protein?
        if (defined $function) {
            # Compute the protein ID.
            my $protID = Shrub::ProteinID($seq);
            # Insert the protein into the database.
            $loader->InsertObject('Protein', id => $protID, sequence => $seq);
            # Insure the function is in the database.
            my ($funcID, $comment) = $self->ProcessFunction($function);
            # Attach the function to it at the current privilege level and all levels
            # below.
            for (my $p = $priv; $p >= 0; $p--) {
                $loader->InsertObject('Protein2Function', 'from-link' => $protID,
                    'to-link' => $funcID, comment => $comment, security => $p);
                $stats->Add(functionLinkInserted => 1);
            }
            # If we are adding translation links, add them here.
            if ($translateLinks) {
                $loader->InsertObject('Protein2Feature', 'to-link' => $pegId,
                        'from-link' => $protID);
                $stats->Add(featureLinkInserted => 1);
            }
        }
    }
}

=head3 ReadFeatures

    my $funcHash = $funcLoader->ReadFeatures($genome, $fileName);

Read the feature information from a tab-delimited feature file. For each feature, the file contains
the feature ID, its location string (Sapling format), and its functional assignment. This method
will insert the feature, connect it to the genome and the contig, then record the functional
assignment in a hash for processing later.

=over 4

=item genome

ID of the genome whose feature file is being processed.

=item fileName

Name of the file containing the feature data to process.

=item RETURN

Returns a reference to a hash mapping each feature ID to the text of its functional assignment.

=back

=cut

sub ReadFeatures {
    # Get the parameters.
    my ($self, $genome, $fileName) = @_;
    # Get the loader object.
    my $loader = $self->{loader};
    # Get the statistics object.
    my $stats = $loader->stats;
    # Open the file for input.
    my $ih = $loader->OpenFile($fileName, 'feature');
    # The return hash will be built in here.
    my %retVal;
    # Loop through the feature file.
    while (my $featureDatum = $loader->GetLine($ih, 'feature')) {
        # Get the feature elements.
        my ($fid, $locString, $function) = @$featureDatum;
        # Create a list of location objects from the location string.
        my @locs = map { BasicLocation->new($_) } split /\s*,\s*/, $locString;
        $stats->Add(featureLocs => scalar(@locs));
        # Store the function in the return hash.
        $retVal{$fid} = $function;
        # Compute the feature type.
        my $ftype;
        if ($fid =~ /fig\|\d+\.\d+\.(\w+)\.\d+/) {
            $ftype = $1;
        } else {
            die "Invalid feature ID $fid.";
        }
        # Compute the total sequence length.
        my $seqLen = 0;
        for my $loc (@locs) {
            $seqLen += $loc->Length;
        }
        # Connect the feature to the genome.
        $loader->InsertObject('Genome2Feature', 'from-link' => $genome, 'to-link' => $fid);
        $loader->InsertObject('Feature', id => $fid, 'feature-type' => $ftype,
                'sequence-length' => $seqLen);
        $stats->Add(feature => 1);
        # Connect the feature to the contigs. This is where the location information figures in.
        my $ordinal = 0;
        for my $loc (@locs) {
            $loader->InsertObject('Feature2Contig', 'from-link' => $fid, 'to-link' => ($genome . ":" . $loc->Contig),
                    begin => $loc->Left, dir => $loc->Dir, len => $loc->Length, ordinal => ++$ordinal);
            $stats->Add(featureSegment => 1);
        }
    }
    # Return the hash of feature IDs to functions.
    return \%retVal;
}

1;