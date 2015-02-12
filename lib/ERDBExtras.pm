#!/usr/bin/perl -w

use strict;
BEGIN {
    eval { require FIG_Config; };
}
no warnings qw(once);

package ERDBExtras;

=head1 ERDB Extras

=head2 Introduction

This module contains parameter declarations used by the ERDB system.

A package called L<FIG_Config> must be somewhere in the PERL path. Parameters
that vary from server to server should go in there. Parameters common to the
entire installation should go in here. Note that if L<FIG_Config> is empty,
everything will still work.

=cut

=head2 Configuration Values

=head3 customERDBtypes

C<$ERDBExtras::customERDBtypes> contains a list of the custom ERDB types associated
with the current code base. This replaces the old L<FIG_Config> variable of the
same name. When a new custom type is created, it should be put in this list.

=cut

our $customERDBtypes        = [qw(ERDBTypeDNA ERDBTypeLink ERDBTypeImage
                               ERDBTypeLongString ERDBTypeSemiBoolean
                               ERDBTypeRectangle ERDBTypeCountVector
                               ERDBTypeProteinData)];

=head3 sort_options

C<$ERDBExtras::sort_options> specifies the options to be used when performing a
sort during a database load. So, for example, if the host machine has a lot of
memory, you can specify a value for the C<-S> option to increase the size of the
sort buffer.

=cut

our $sort_options           = $FIG_Config::sort_options || "";

=head3 temp

C<$ERDBExtras::temp> specifies the name of the directory to be used for
temporary files. It should be a location that is writable and accessible
from the web, because it is used to store images (see L</temp_url>).

=cut

our $temp                   = $FIG_Config::temp || "/tmp";

=head3 temp_url

C<$ERDBExtras::temp_url> must be the URL that can be used to find the temporary
directory from the web (see L</temp>).

=cut

our $temp_url               = $FIG_Config::temp_url || "/tmp";

=head3 delete_limit

C<$ERDBExtras::delete_limit> specifies the maximum number of database rows that should
be deleted at a time. If a non-zero value is specified, SQL deletes will be
limited to the specified size. Use this parameter if large deletes are locking
the database server for unacceptable periods.

=cut

our $delete_limit           = 0;

=head3 diagram_url

C<$ERDBExtras::diagramURL> specifies the URL of the ERDB diagramming engine.
This is a compiled flash movie file (SWF) used for the documentation widget.

=cut

our $diagramURL             = $FIG_Config::diagramURL || "lib/Diagrammer.swf";

=head3 query_limit

C<$ERDBExtras::query_limit> specifies the maximum number of rows that can be
returned by the query script if the user is not authorized. This is used
to prevent denial-of-service attackes against the query engine.

=cut

our $query_limit            = 1000;

=head3 query_retries

C<$ERDBExtras::query_retries> specifies the number of times a lost connection
should be retried when querying the database.

=cut

our $query_retries          = 1;

=head3 sleep_time

C<$ERDBExtras::sleep_time> specifies how many seconds to wait between database
reconnection attempts.

=cut

our $sleep_time             = 10;

1;
