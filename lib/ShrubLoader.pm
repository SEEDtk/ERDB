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


package ShrubLoader;

    use strict;
    use Stats;
    use SeedUtils;
    use Digest::MD5;
    use Carp;

=head1 Shrub Load Utilities

=head2 Introduction

This object manages simple utilities for loading the Shrub database. It contains the following
fields.

=over 4

=item shrub

The L<Shrub> object itself.

=item tables

Reference to a hash mapping each object being loaded to a table management object. The table management object
contains the file handles for the object's load files (in the B<handles> member), the maps for the object's
relations (in the B<maps> member), and the relation names (in the B<names> member). Each of these members is
coded as a list reference, in parallel order.

=item tableList

Reference to a list of the tables in the order they are supposed to be loaded, which is the order
they were passed in to L</Open>.

=item stats

L<Stats> object containing the statistics for this load.

=item hashes

Reference to a hash mapping entity names to hashes that cache the content of the entity. The entity must
be of the type that stores a string that is identified by an autonumber ID. For each entity, this hash
contains the unqualified name of the text field and a sub-hash that maps MD5s of the text field to IDs.
If the string is already in the database, the hash can be used to retrieve the ID; otherwise, we know
we need to add the string to the database.

=item replaces

Reference to a hash containing the names of the tables being inserted in replace mode.

=back

=head2 Special Methods

=head3 new

    my $loader = ShrubLoader->new($shrub);

Create a new, blank loader object.

=over 4

=item shrub

L<Shrub> object for the database being loaded.

=back

=cut

sub new {
    # Get the parameters.
    my ($class, $shrub) = @_;
    # Create the object with the shrub database attached and no tables being loaded.
    my $retVal = { shrub => $shrub, stats => Stats->new(), hashes => {},
                   tables => { }, replaces => { }, tableList => [] };
    # Bless and return it.
    bless $retVal, $class;
    return $retVal;
}

=head2 Access Methods

=head3 db

    my $shrub = $loader->db;

Return the L<Shrub> database object.

=cut

sub db {
    my ($self) = @_;
    return $self->{shrub};
}


=head3 stats

    my $stats = $loader->stats;

Return the statistics object.

=cut

sub stats {
    my ($self) = @_;
    return $self->{stats};
}

=head2 File Utility Methods

=head3 OpenDir

    my @files = ShrubLoader::OpenDir($dirName, $filtered, $flag);

 or

     my @files = $loader->OpenDir($dirName, $filtered, $flag);

Open a directory and return all the file names. This function essentially performs
the functions of an C<opendir> and C<readdir>. If the I<$filtered> parameter is
set to TRUE, all filenames beginning with a period (C<.>), dollar sign (C<$>),
or pound sign (C<#>) and all filenames ending with a tilde C<~>) will be
filtered out of the return list. If the directory does not open and I<$flag> is not
set, an exception is thrown. So, for example,

    my @files = OpenDir("/Volumes/fig/contigs", 1);

is effectively the same as

    opendir(TMP, "/Volumes/fig/contigs") || die("Could not open /Volumes/fig/contigs.");
    my @files = grep { $_ !~ /^[\.\$\#]/ && $_ !~ /~$/ } readdir(TMP);

Similarly, the following code

    my @files = grep { $_ =~ /^\d/ } OpenDir("/Volumes/fig/orgs", 0, 1);

Returns the names of all files in C</Volumes/fig/orgs> that begin with digits and
automatically returns an empty list if the directory fails to open.

=over 4

=item dirName

Name of the directory to open.

=item filtered

TRUE if files whose names begin with a period (C<.>) should be automatically removed
from the list, else FALSE.

=item flag

TRUE if a failure to open is okay, else FALSE

=back

=cut

sub OpenDir {
    # Convert the instance-style call to a direct call.
    shift if UNIVERSAL::isa($_[0],__PACKAGE__);
    # Get the parameters.
    my ($dirName, $filtered, $flag) = @_;
    # Declare the return variable.
    my @retVal = ();
    # Open the directory.
    if (opendir(my $dirHandle, $dirName)) {
        # The directory opened successfully. Get the appropriate list according to the
        # strictures of the filter parameter.
        if ($filtered) {
            @retVal = grep { $_ !~ /^[\.\$\#]/ && $_ !~ /~$/ } readdir $dirHandle;
        } else {
            @retVal = readdir $dirHandle;
        }
        closedir $dirHandle;
    } elsif (! $flag) {
        # Here the directory would not open and it's considered an error.
        die "Could not open directory $dirName.";
    }
    # Return the result.
    return @retVal;
}

=head3 GetNamesFromFile

    my $names = $loader->GetNamesFromFile($fileName, $type);

Read the names or IDs found in the first column of the specified tab-delimited file.

=over 4

=item fileName

Name of the file to read.

=item type

The type of name found in the file. This must be a singular noun and will be used in error messages and
statistics.

=item RETURN

Returns a reference to a list of names taken from the first column of each record in the file.

=back

=cut

sub GetNamesFromFile {
    # Get the parameters.
    my ($self, $fileName, $type) = @_;
    # Get the statistics object.
    my $stats = $self->{stats};
    # Open the file for input.
    open(my $ih, "<$fileName") || die "Could not open $type input file $fileName: $!";
    # We will put the names in here.
    my @retVal;
    # Loop through the file records.
    while (! eof $ih) {
        my $line = <$ih>;
        chomp $line;
        my ($name) = split /\t/, $line;
        # Ignore empty values.
        if (defined $name && $name ne "") {
            push @retVal, $name;
            $stats->Add("$type-in" => 1);
        }
    }
    # Return the list of names.
    return \@retVal;
}

=head3 OpenFasta

    my $fh = $loader->OpenFasta($fileName, $type);

Open a FASTA file for input. This returns an object that can be passed to L</GetLine> as a file handle.

=over 4

=item fileName

Name of the FASTA file to open.

=item type

Type of sequence in the file. This must be a singular noun, and will be used in error messages and statistics.

=item RETURN

Returns an object (in this case an array reference containing (0) the file handle, (1) the ID, and (2) the comment)
that can be passed to L</GetLine> to read from the FASTA.

=back

=cut

sub OpenFasta {
    # Get the parameters.
    my ($self, $fileName, $type) = @_;
    # Get the statistics object.
    my $stats = $self->{stats};
    # Open the file for input.
    open(my $ih, "<$fileName") || die "Could not open $type FASTA file: $!";
    $stats->Add("$type-file" => 1);
    # This will be our return list.
    my @retVal = ($ih);
    # Is the file empty?
    if (! eof $ih) {
        # No, read the first line.
        my $line = <$ih>;
        chomp $line;
        if ($line =~ /^>(\S*)\s*(.*)/) {
            # Here we have a valid header. Save the ID and comment.
            push @retVal, $1, $2;
        } else {
            # Here we do not have a valid header.
            die "Invalid header in FASTA file $fileName.";
        }
    }
    # Return the file descriptor.
    return \@retVal;
}

=head3 OpenFile

    my $ih = $loader->OpenFile($fileName, $type);

Open the specified file for input. If the file does not open, an error will be thrown.

=over 4

=item fileName

Name of the file to open.

=item type

The type of record found in the file. This must be a singular noun, and will be used in error messages and
statistics.

=item RETURN

Returns an open file handle.

=back

=cut

sub OpenFile {
    # Get the parameters.
    my ($self, $fileName, $type) = @_;
    # Get the statistics object.
    my $stats = $self->{stats};
    # Open the file for input.
    open(my $retVal, "<$fileName") || die "Could not open $type file: $!";
    $stats->Add("$type-file" => 1);
    # Return the handle.
    return $retVal;
}

=head3 GetLine

    my $fields = $loader->GetLine($ih, $type);

Read a line of data from an open tab-delimited or FASTA file.

=over 4

=item ih

Open input handle for the file or a FASTA object returned from L</OpenFasta>.

=item type

The type of record found in the file. This must be a singular noun, and will be used in error messages and
statistics.

=item RETURN

Returns a reference to a list of the tab-separated fields in the current line of the file, or C<undef>
if end-of-file was read.

=back

=cut

sub GetLine {
    # Get the parameters.
    my ($self, $ih, $type) = @_;
    # Get the statistics object.
    my $stats = $self->{stats};
    # The fields read will be put in here.
    my $retVal;
    # The data line will be put in here.
    my $line;
    # Determine the type of operation.
    if (ref $ih ne 'ARRAY') {
        # Here we have a tab-delimited file. Do we have a line of data?
        if (! eof $ih) {
            # Yes, Extract the fields.
            $line = <$ih>;
            chomp $line;
            $stats->Add("$type-lineIn" => 1);
            $retVal = [split /\t/, $line];
        }
    } else {
        # Here we have a FASTA file. Get the FASTA information.
        my ($fh, $id, $comment) = @$ih;
        # Only proceed if we are not already past end-of-file.
        if (defined $id) {
            # Here we are positioned on a data line. Loop until we
            # run out of data lines and hit a header.
            my @data;
            my $header = 0;
            while (! eof $fh && ! $header) {
                $line = <$fh>;
                chomp $line;
                $stats->Add("$type-lineIn" => 1);
                if (substr($line, 0, 1) eq '>') {
                    # Here we have a header.
                    $header = 1;
                } else {
                    # More data. Save it.
                    push @data, $line;
                }
            }
            # Here we are at the start of a new record. Output the old one.
            $retVal = [$id, $comment, join("", @data)];
            $stats->Add("$type-fastaRecord" => 1);
            # If there is another record coming, set up for it.
            if ($header) {
                $line =~ /^>(\S*)\s*(.*)/;
                @{$ih}[1, 2] = ($1, $2);
            } else {
                # End-of-file. Insure we know it.
                @{$ih}[1, 2] = (undef, "");
            }
        }
    }
    # Return the line.
    return $retVal;
}

=head3 ReadMetaData

    my $metaHash = $loader->ReadMetaData($fileName, %options);

Read a metadata file into a hash. A metadata file contains keywords and values, one pair per line, using a single
colon as a field separator.

=over 4

=item fileName

Name of the metadata file to read.

=item options

Hash of options. The valid keywords are

=over 8

=item required

Maps to a list reference of required keywords. If one of the keywords is not found in the metadata file,
an error will occur.

=back

=item RETURN

Returns a reference to a hash that maps each keyword in the metadata file to its value.

=back

=cut

sub ReadMetaData {
    # Get the parameters.
    my ($self, $fileName, %options) = @_;
    # Get the statistics object.
    my $stats = $self->{stats};
    # Open the file for input.
    my $ih = $self->OpenFile($fileName, 'metadata');
    # Read each line and parse into the return hash.
    my %retVal;
    while (! eof $ih) {
        my $line = <$ih>;
        $stats->Add('metadata-line' => 1);
        chomp $line;
        if ($line =~ /^([^:]+):(.+)/) {
            $retVal{$1} = $2;
        } else {
            die "Invalid line in metadata file $fileName.";
        }
    }
    # If there are required keywords, check for them here.
    my $list = $options{required};
    if (defined $list) {
        # Insure we have a list of keywords.
        if (ref $list ne 'ARRAY') {
            $list = [$list];
        }
        # Loop through the required keywords.
        for my $key (@$list) {
            if (! defined $retVal{$key}) {
                die "Missing required keyword $key in metadata file $fileName.";
            }
        }
    }
    # Return the hash of key-value pairs.
    return \%retVal;
}

=head3 WriteMetaData

    $loader->WriteMetaData($fileName, \%metaHash);

Write the metadata specified by a hash to the specified file.

=over 4

=item fileName

Name of the file to which the metadata should be written.

=item metaHash

Hash containing the key-value pairs to be output to the file. For each entry in the hash,
a line will be written to the output file containing the key and the value, colon-separated.

=back

=cut

sub WriteMetaData {
    # Get the parameters.
    my ($self, $fileName, $metaHash) = @_;
    # Get the statistics object.
    my $stats = $self->{stats};
    # Open the output file.
    open(my $oh, ">$fileName") || die "Could not open metadata output file $fileName: $!";
    $stats->Add(metaFileOut => 1);
    # Loop through the hash, writing key/value lines.
    for my $key (sort keys %$metaHash) {
        my $value = $metaHash->{$key};
        print $oh "$key:$value\n";
        $stats->Add(metaLineOut => 1);
    }
    # Close the output file.
    close $oh;
}

=head2 General Loader Utility Methods

=head3 md5

    my $md5 = ShrubLoader::md5($string);

or

    my $md5 = $loader->md5($string);

Return the MD5 digest of a string. This is the standard hex MD5 used for protein sequences, but it can
be applied to any text string.

=over 4

=item string

String to digest.

=item RETURN

Returns a digested copy of the string. Two different strings will almost certainly have two
different digest values.

=back

=cut

sub md5 {
    # Convert the instance-style call to a direct call.
    shift if UNIVERSAL::isa($_[0],__PACKAGE__);
    # Get the parameters.
    my ($self, $string) = @_;
    # Compute the digest.
    my $retVal = Digest::MD5::md5_hex($string);
    # Return the result.
    return $retVal;
}

=head3 FindGenomeList

    my $genomeHash = $loader->FindGenomeList($repository, %options);

Find all the genomes in the specified repository directory. The result will list all the genome directories
and describe where to find the genomes. The genomes could be in a single flat directory or in a hierarchy that
we must drill down, so there is some recursion involved.

=over 4

=item repository

Directory name of the genome repository.

=item options

A hash of tuning options. The following keys are accepted.

=over 8

=item useDirectory

If TRUE, then any index file in the repository will be ignored and the directory hierarchy will be traversed.
Otherwise, the index file will be read if present.

=item RETURN

Returns a reference to a hash mapping genome IDs to directory names.

=back

=cut

sub FindGenomeList {
    # Get the parameters.
    my ($self, $repository, %options) = @_;
    # The output will be put in here.
    my %retVal;
    # Can we use an index file?
    my $indexUsed;
    if (! $options{useDirectory} && -f "$repository/index") {
        # Open the index file.
        if (! open(my $ih, "<$repository/index")) {
            print "Error opening $repository index file: $!\n";
        } else {
            # We have the index file. Read the genomes from it.
            print "Reading genome index for $repository.\n";
            while (my $fields = $self->GetLine($ih, 'GenomeIndex')) {
                my ($genome, undef, $dir) = @$fields;
                $retVal{$genome} = "$repository/$dir";
            }
            # Denote we've loaded from the index.
            $indexUsed = 1;
        }
    }
    # Did we use the index file?
    if (! $indexUsed) {
        # No index file, we need to traverse the tree. This is a stack of directories still to process.
        my $genomeCount = 0;
        my @dirs = ($repository);
        while (@dirs) {
            # Get the next directory to search.
            my $dir = pop @dirs;
            # Retrieve all the subdirectories. This is a filtered search, so "." and ".." are skipped
            # automatically.
            my @subDirs = grep { -d "$dir/$_" } OpenDir($dir, 1);
            # Loop through the subdirectories.
            for my $subDir (@subDirs) {
                # Compute the directory name.
                my $dirName = "$dir/$subDir";
                # Check to see if this is a genome.
                if ($subDir =~ /^\d+\.\d+$/) {
                    # Here we have a genome directory.
                    $retVal{$subDir} = $dirName;
                    $genomeCount++;
                    if ($genomeCount % 200 == 0) {
                        print "Reading genome directories. $genomeCount genomes processed.\n";
                    }
                } else {
                    # Here we have a subdirectory that might contain more genomes.
                    # Push it onto the stack to be processed later.
                    push @dirs, $dirName;
                }
            }
        }
        print "$genomeCount genomes found in $repository.\n";
    }
    # Return the genome hash.
    return \%retVal;
}

=head3 FindSubsystem

    my $subDir = ShrubLoader::FindSubsystem($subsysDirectory, $subName);

or

    my $subDir = $loader->FindSubsystem($subsysDirectory, $subName);

Find the directory for the specified subsystem in the specified subsystem repository. Subsystem
directory names are formed by converting spaces in the subsystem name to underscores and using
the result as a directory name under the subsystem repository directory. This method will fail if
the subsystem directory is not found.

=over 4

=item subsysDirectory

Name of the subsystem repository directory.

=item subName

Name of the target subsystem.

=item RETURN

Returns the name of the directory containing the subsystem source files.

=back

=cut

sub FindSubsystem {
    # Convert the instance-style call to a direct call.
    shift if UNIVERSAL::isa($_[0],__PACKAGE__);
    # Get the parameters.
    my ($subsysDirectory, $subName) = @_;
    # Convert the subsystem name to a directory format.
    my $fixedName = $subName;
    $fixedName =~ tr/ /_/;
    # Form the full directory name.
    my $retVal = "$subsysDirectory/$fixedName";
    # Verify that it exists.
    if (! -d $retVal) {
        die "Subsystem $subName not found in $subsysDirectory.";
    }
    # Return the directory name.
    return $retVal;
}

=head3 Check

    my $found = $loader->Check($entity, $id, $entityHash);

Check to determine if a particular entity instance is in the database. This task is normally performed
by the L<ERDB/Exists> function. In this case, the caller can optionally specify a reference to a hash
containing all the IDs in the database, to improve performance. If the hash is not present, this method
falls back to the B<Exists> call.

=over 4

=item entity

Name of the entity to check.

=item id

ID of the instance for which to look.

=item entityHash

If specified, a reference to a hash whose keys are all the IDs of the entity in the database.
If unspecified, the database will be interrogated directly.

=item RETURN

Returns TRUE if an entity instance exists with the specified, ID, else FALSE.

=back

=cut

sub Check {
    # Get the paramteers.
    my ($self, $entity, $id, $entityHash) = @_;
    # This will be the return value.
    my $retVal;
    # Do we have a hash?
    if ($entityHash) {
        # Yes, check it.
        $retVal = $entityHash->{$id};
    } else {
        # No, check the database.
        $retVal = $self->{shrub}->Exists($entity => $id);
    }
    # Return the determination indicator.
    return $retVal;
}


=head2 In-Memory Loader Table Utilities

=head3 CreateTableHash

    my $tableHash = $loader->CreateTableHash($table, $textField);

Create a hash table that maps checksums to IDs. The checksums are taken from a named field in an entity object,
which should be unqualified (i.e. C<sequence> instead of C<Protein(sequence))>).

=over 4

=item table

Name of the table from which the hash is to be created. Every record in the table will be read.

=item textField

Unqualified name of the text field containing the checksums.

=item RETURN

Returns a reference to a hash that maps MD5s of the specified text field to entity IDs from the specifed table.
The hash is also stored in this object for use by the L</InsureTable> method.

=back

=cut

sub CreateTableHash {
    # Get the parameters.
    my ($self, $table, $textField) = @_;
    # Get the database object.
    my $shrub = $self->{shrub};
    # Get the statistics object.
    my $stats = $self->{stats};
    # Format the field name.
    my $textFieldName = "$table($textField)";
    # Create the query to build the hash.
    my $query = $shrub->Get($table, "", []);
    # We will build the hash in here.
    my %retVal;
    my $count = 0;
    # Loop through the records, filling in the hash.
    while (my $record = $query->Fetch()) {
        my ($id, $text) = $record->Values(['id', $textFieldName]);
        $retVal{$text} = $id;
        $count++;
    }
    # Update the statistics.
    $stats->Add("$table-hash" => $count);
    # Save the hash.
    $self->{hashes}{$table} = [$textField, \%retVal];
    # Return the hash.
    return \%retVal;
}

=head3 InsureTable

    my ($id, $newFlag) = $loader->InsureTable($table, %fields);

This is a wrapper for the L<ERDB/InsertNew> method that first checks the cache created by L</CreateTableHash>.
If the specified entity does not exist, it is inserted into the database. Otherwise, its ID is returned from
the hash.

=over 4

=item table

Name of the relevant entity.

=item fields

Hash of the fields to store in the entity, excluding the ID field.

=item RETURN

Returns a two-element list. The first element is the ID assigned to the entity instance inserted or found.
The second element is TRUE if the entity instance was inserted and FALSE otherwise.

=back

=cut

sub InsureTable {
    # Get the parameters.
    my ($self, $table, %fields) = @_;
    # This will be the ID return value.
    my $retVal;
    # This will be set to TRUE if we insert a record.
    my $newFlag = 0;
    # Get the database object.
    my $shrub = $self->{shrub};
    # Get the statistics object.
    my $stats = $self->{stats};
    # Get the table's descriptor from the hashes member.
    my $descriptor = $self->{hashes}{$table};
    if (! defined $descriptor) {
        die "InsureTable called for entity $table that was not initialized with CreateTableHash.";
    } else {
        # We got the descriptor. Parse out the pieces.
        my ($textField, $hashTable) = @$descriptor;
        # Get the text field value.
        my $textValue = $fields{$textField};
        # Check the hash.
        $retVal = $hashTable->{$textValue};
        if (defined $retVal) {
            # We found it, we're done.
            $stats->Add("$table-foundRecord" => 1);
        } else {
            # We need to add a record.
            $retVal = $shrub->InsertNew($table, %fields);
            $stats->Add("$table-newRecord" => 1);
            # Update the hash.
            $hashTable->{$textValue} = $retVal;
            # Denote we inserted a record.
            $newFlag = 1;
        }
    }
    # Return the ID and the insert flag.
    return ($retVal, $newFlag);
}


=head2 Table-Loading Utility Methods

=head3 Clear

    $loader->Close(@tables);

Clear the database relations for the specified objects.

=over 4

=item tables

List of the names of the objects whose data is to be cleared from the database.

=back

=cut

sub Clear {
    # Get the parameters;
    my ($self, @tables) = @_;
    # Get the database object.
    my $shrub = $self->{shrub};
    # Get the statistics object.
    my $stats = $self->{stats};
    # Loop through the tables specified.
    for my $table (@tables) {
        # Get the descriptor for this object.
        my $object = $shrub->FindEntity($table);
        if ($object) {
            $stats->Add(entityClear => 1);
        } else {
            $object = $shrub->FindRelationship($table);
            if (! $object) {
                die "$table is not a valid entity or relationship name.";
            } else {
                $stats->Add(relationshipClear => 1);
            }
        }
        print "Clearing $table.\n";
        # Get the hash of relations.
        my $relHash = $object->{Relations};
        # Loop through them.
        for my $rel (keys %$relHash) {
            # Recreate this relation.
            $shrub->CreateTable($rel, 1);
            print "$rel recreated.\n";
            $stats->Add(tableClear => 1);
        }
    }
}

=head3 Open

    $loader->Open(@tables);

Open the load files for one or more entities and/or relationships.

=over 4

=item tables

List of the names of the objects to be loaded.

=back

=cut

sub Open {
    # Get the parameters.
    my ($self, @tables) = @_;
    # Get the database object.
    my $shrub = $self->{shrub};
    # Get the statistics object.
    my $stats = $self->{stats};
    # Get the current tables hash and list.
    my $tableH = $self->{tables};
    my $tableL = $self->{tableList};
    # Compute the load directory.
    my $loadDir = $shrub->LoadDirectory();
    # Loop through the tables specified.
    for my $table (@tables) {
        # Only proceed if this table is not already set up.
        if (exists $tableH->{$table}) {
            warn "$table is being opened for loading more than once.\n";
            $stats->Add(duplicateOpen => 1);
        } else {
            # The file handles will be put in here.
            my @handles;
            # The relation maps will be put in here.
            my @maps;
            # The relation names will be put in here.
            my @names;
            # Get the descriptor for this object.
            my $object = $shrub->FindEntity($table);
            if ($object) {
                $stats->Add(entityOpen => 1);
            } else {
                $object = $shrub->FindRelationship($table);
                if (! $object) {
                    die "$table is not a valid entity or relationship name.";
                } else {
                    $stats->Add(relationshipOpen => 1);
                }
            }
            print "Opening $table.\n";
            # Get the hash of relations.
            my $relHash = $object->{Relations};
            # Loop through them.
            for my $rel (keys %$relHash) {
                # Get this relation's field descriptor.
                push @maps, $relHash->{$rel}{Fields};
                # Open a file for it.
                my $fileName = "$loadDir/$rel.dtx";
                open(my $ih, ">$fileName") || die "Could not open load file $fileName: $!";
                $stats->Add(fileOpen => 1);
                push @handles, $ih;
                # Save its name.
                push @names, $rel;
                print "$rel prepared for loading.\n";
            }
            # Store the load information.
            $tableH->{$table} = { handles => \@handles, maps => \@maps, names => \@names };
            push @$tableL, $table;
        }
    }
}

=head3 ReplaceMode

    $loader->ReplaceMode(@tables);

Denote that the specified objects should be processed in replace mode instead of ignore mode. In
replace mode, inserted rows replace existing duplicate rows rather than being discarded.

=over 4

=item tables

List of the names of the entities and relationships to be processed in replace mode.

=back

=cut

sub ReplaceMode {
    # Get the parameters.
    my ($self, @tables) = @_;
    # Get the replace-mode hash.
    my $repHash = $self->{replaces};
    # Loop through the object names.
    for my $table (@tables) {
        # Mark this object as replace mode.
        $repHash->{$table} = 1;
    }
}

=head3 InsertObject

    $loader->InsertObject($table, %fields);

Insert the specified object into the load files.

=over 4

=item table

Name of the object (entity or relationship) being inserted.

=item fields

Hash mapping field names to values. Multi-value fields are passed as list references. All fields should already
be encoded for insertion.


=back

=cut

sub InsertObject {
    # Get the parameters.
    my ($self, $table, %fields) = @_;
    # Get the statistics object.
    my $stats = $self->{stats};
    # Get the load object for this table.
    my $loadThing = $self->{tables}{$table};
    # Are we loading this object using a load file?
    if (! $loadThing) {
        # No, we must insert it directly. Get the database object.
        my $shrub = $self->{shrub};
        # Compute the duplicate-record mode.
        my $dup = ($self->{replaces}{$table} ? 'replace' : 'ignore');
        $shrub->InsertObject($table, \%fields, encoded => 1, dup => $dup);
        $stats->Add("$table-insert" => 1);
    } else {
        # Yes, we need to output to the load files. Loop through the relation tables in the load thing.
        my $handles = $loadThing->{handles};
        my $maps = $loadThing->{maps};
        my $names = $loadThing->{names};
        my $n = scalar @$handles;
        for (my $i = 0; $i < $n; $i++) {
            my $handle = $handles->[$i];
            my $map = $maps->[$i];
            # Figure out if this is the primary relation.
            if ($names->[$i] eq $table) {
                # It is. Loop through the fields of this relation and store the values in here.
                my @values;
                for my $field (@$map) {
                    # Check for the field in the field hash.
                    my $name = $field->{name};
                    my $value = $fields{$name};
                    if (! defined $value && ! $field->{null}) {
                        # We have a missing field value, and we need to identify it. Start with the table name. Add
                        # an ID if we have one.
                        my $tName = $table;
                        if (defined $fields{id}) {
                            $tName = "$tName record $fields{id}";
                        } elsif (defined $fields{'from-link'}) {
                            $tName = "$tName for " . $fields{'from-link'} . " to " . $fields{'to-link'};
                        }
                        confess "Missing value for $name in $tName.";
                    } else {
                        # Store this value.
                        push @values, $value;
                    }
                }
                # Write the primary record.
                print $handle join("\t", @values) . "\n";
                $stats->Add("$table-record" => 1);
            } else {
                # Here we have a secondary relation. A secondary always has two fields, the ID and a multi-value
                # field which will come to us as a list.
                my $id = $fields{id};
                if (! defined $id) {
                    die "ID missing in output attempt of $table.";
                }
                # Get the secondary value.
                my $values = $fields{$map->[1]{name}};
                # Insure it is a list.
                if (! defined $values) {
                    $values = [];
                } elsif (ref $values ne 'ARRAY') {
                    $values = [$values];
                }
                # Loop through the values, writing them out.
                for my $value (@$values) {
                    print $handle "$id\t$value\n";
                    $stats->Add("$table-value" => 1);
                }
            }
        }
    }
}

=head3 Close

    $loader->Close();

Close and load all the load files being created.

=cut

sub Close {
    # Get the parameters.
    my ($self) = @_;
    # Get the database object.
    my $shrub = $self->{shrub};
    # Get the load directory.
    my $loadDir = $shrub->LoadDirectory();
    # Get the replace-mode hash.
    my $repHash = $self->{replaces};
    # Get the statistics object.
    my $stats = $self->{stats};
    # Get the load hash and the list of tables.
    my $loadThings = $self->{tables};
    my $loadList = $self->{tableList};
    # Loop through the objects being loaded.
    for my $table (@$loadList) {
        my $loadThing = $loadThings->{$table};
        print "Closing $table.\n";
        # Loop through the relations for this object.
        my $names = $loadThing->{names};
        my $handles = $loadThing->{handles};
        my $dups = $loadThing->{dups};
        my $n = scalar @$names;
        for (my $i = 0; $i < $n; $i++) {
            my $name = $names->[$i];
            my $handle = $handles->[$i];
            # Close the file.
            close $handle;
            # Compute the duplicate-record mode.
            my $dup = ($repHash->{$name} ? 'replace' : 'ignore');
            # Load it into the database.
            print "Loading $name.\n";
            my $newStats = $shrub->LoadTable("$loadDir/$name.dtx", $name, dup => $dup);
            # Merge the statistics.
            $stats->Accumulate($newStats);
        }
    }
}

1;
