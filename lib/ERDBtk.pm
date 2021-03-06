package ERDBtk;

    use strict;
    use base qw(Exporter);
    use vars qw(@EXPORT_OK);
    @EXPORT_OK = qw(encode);
    use StringUtils;
    use Data::Dumper;
    use XML::Simple;
    use ERDBtk::Query;
    use ERDBtk::Object;
    use Stats;
    use Time::HiRes qw(gettimeofday);
    use Digest::MD5 qw(md5_base64);
    use CGI qw(-nosticky);
    use ERDBtk::Helpers::SQLBuilder;
    use ERDBtk::Helpers::ObjectPath;
    use ERDBtkExtras;
    use FreezeThaw;

=head1 Entity-Relationship Database Package

=head2 Introduction

The Entity-Relationship Database Package allows the client to create an
easily-configurable database of Entities connected by Relationships. Each entity
is represented by one or more relations in an underlying SQL database. Each
relationship is represented by a single relation that connects two entities.
Entities and relationships are collectively referred to in the documentation as
I<objects>.

Although this package is designed for general use, most examples are derived
from the world of bioinformatics, which is where this technology was first
deployed.

Each entity has at least one relation, the I<primary relation>, that has the
same name as the entity. The primary relation contains a field named C<id> that
contains the unique identifier of each entity instance. An entity may have
additional relations that contain fields which are optional or can occur more
than once. For example, the C<Feature> entity has a B<feature-type> attribute
that occurs exactly once for each feature. This attribute is implemented by a
C<feature_type> column in the primary relation C<Feature>. In addition, however,
a feature may have zero or more aliases. These are implemented using a
C<FeatureAlias> relation that contains two fields-- the feature ID (C<id>) and
the alias name (C<alias>). The C<Feature> entity also contains an optional
virulence number. This is implemented as a separate relation C<FeatureVirulence>
which contains an ID (C<id>) and a virulence number (C<virulence>). If the
virulence of a feature I<ABC> is known to be 6, there will be one row in the
C<FeatureVirulence> relation possessing the value I<ABC> as its ID and 6 as its
virulence number. If the virulence of I<ABC> is not known, there will not be any
rows for it in C<FeatureVirulence>.

Entities are connected by binary relationships implemented using single
relations possessing the same name as the relationship itself and that has an
1-to-many (C<1M>) or many-to-many (C<MM>). Each relationship's relation contains
a C<from-link> field that contains the ID of the source entity and a C<to-link>
field that contains the ID of the target entity. The name of the relationship is
generally a verb phrase with the source entity as the subject and the target
entity as the object. So, for example, the B<ComesFrom> relationship connects
the C<Genome> and C<Source> entities, and indicates that a particular source
organization participated in the mapping of the genome. A source organization
frequently participates in the mapping of many genomes, and many source
organizations can cooperate in the mapping of a single genome, so this
relationship has an arity of many-to-many (C<MM>). The relation that implements
the C<ComesFrom> relationship is called C<ComesFrom> and contains two fields--
C<from-link>, which contains a genome ID, and C<to-link>, which contains a
source ID.

A one-to-many relationship can be I<embedded> in the entity on the many side.
So, for example, A genome contains many features but a feature is in only one
genom. The C<IsOwnerOf> relationship from C<Genome> to C<Feature> can be
stored as a field in the C<Feature> entity. The field C<IsOwnerOf(from-link)>
is a new field in C<Feature>, and the field C<IsOwnerOf(to-link)> is actually
the feature ID. Embedded relationships cannot have a C<ToIndex> or any other
alternatate indexes.

A relationship may itself have attributes. These attributes, known as
I<intersection data attributes>, are implemented as additional fields in the
relationship's relation. So, for example, the B<IsMadeUpOf> relationship
connects the B<Contig> entity to the B<Sequence> entity, and is used to
determine which sequences make up a contig. The relationship has as an attribute
the B<start-position>, which indicates where in the contig that the sequence
begins. This attribute is implemented as the C<start_position> field in the
C<IsMadeUpOf> relation.

The database itself is described by an XML file. In addition to all the data
required to define the entities, relationships, and attributes, the schema
provides space for notes describing the data and what it means and information
about how to display a diagram of the database. These are used to create web
pages describing the data.

=head2 Data Types, Queries and Filtering

=head3 Data Types

The ERDBtk system supports many different data types. It is possible to
configure additional user-defined types by adding PERL modules to the
code. Each new type must be a subclass of L<ERDBtk::Type>. Standard
types are listed in the compile-time STANDARD_TYPES constant. Custom
types should be listed in the C<$ERDBtkExtras::customERDBtktypes> variable
of the configuration file. The variable must be a list reference
containing the names of the ERDBtk::Type subclasses for the custom
types.

To get complete documentation of all the types, use
the L</ShowDataTypes> method. The most common types are

=over 4

=item int

Signed whole number with a range of roughly negative 2 billion to positive
2 billion. Integers are stored in the database as a 32-bit binary number.

=item string

Variable-length string, up to around 250 characters. Strings are stored in
the database as variable-length ASCII with some escaping.

=item text

Variable-length string, up to around 65000 characters. Text is stored in the
database as variable-length ASCII with some escaping. Only the first 250
characters can be indexed.

=item float

Double-precision floating-point number, ranging from roughly -10^-300
to 10^-300, with around 14 significant digits. Floating-point numbers
are stored in the database in IEEE 8-byte floating-point format.

=item date

Date/time value, in whole seconds. Dates are stored as a number of seconds
from the beginning of the Unix epoch (January 1, 1970) in Universal
Coordinated Time. This makes it identical to a date or time number in PERL,
Unix, or Windows.

=back

All data fields are converted when stored or retrieved using the
L</EncodeField> and L</DecodeField> methods. This allows us to store
very exotic data values such as string lists, images, and PERL objects. The
conversion is not, however, completely transparent because no conversion
is performed on the parameter values for the various L</Get>-based queries.
There is a good reason for this: you can specify general SQL expressions as
filters, and it's extremely difficult for ERDBtk to determine the data type of
a particular parameter. This topic is dealt with in more detail below.

=head3 Standard Field Name Format

There are several places in which field names are specified by the caller.
The standard field name format is the name of the entity or relationship
followed by the field name in parentheses. In some cases there a particular
entity or relationship is considered the default. Fields in the default
object can be specified as an unmodified field name. For example,

    Feature(species-name)

would specify the species name field for the C<Feature> entity. If the
C<Feature> table were the default, it could be specified as

    species-name

without the object name.

In some cases, the object name may not be the actual name of an object
in the database. It could be an alias assigned by a query, or the converse
name of a relationship. Alias names and converse names are generally
specified in the object name list of a query method. The alias or converse
name used in the query method will be carried over in all parameters to the
method and any data value structures returned by the query. In most cases,
once you decide on a name for something in a query, the name will stick for
all data returned by the query.

=head3 Queries

Queries against the database are performed by variations of the L</Get> method.
This method has four parameters: the I<object name list>, the I<filter clause>,
the I<parameter list>, and an optional I<field list>. There is a certain complexity
involved in queries that has evolved over a period of many years in which the needs
of the applications were balanced against a need for simplicity. In most cases, you
just list the objects used in the query, code a standard SQL filter clause with
field names in the L</Standard Field Name Format>, and specify a list of
parameters to plug in to the parameter marks. The use of the special field name
format and the list of object names spare you the pain of writing a C<FROM>
clause and worrying about joins. For example, here's a simple query to look up
all Features for a particular genome.

    my $query = $erdb->Get('Genome HasFeature Feature', 'Genome(id) = ?', [$genomeID]);

For more complicated queries, see the rest of this section.

=head4 Object Name List

The I<object name list> specifies the names of the entities and relationships
that participate in the query. This includes every object used to filter the
query as well as every object from which data is expected. The ERDBtk engine will
automatically generate the join clauses required to make the query work, which
greatly simplifies the coding of the query. You can specify the object name
list using a list reference or a space-delimited string. The following two
calls are equivalent.

    my $query = $erdb->Get(['Genome', 'UsesImage', 'Image'], $filter, \@parms);

    my $query = $erdb->Get('Genome UsesImage Image', $filter, \@parms);

If you specify a string, you have a few more options.

=over 4

=item *

You can use the keyword C<AND> to start a new join chain with an object
further back in the list.

=item *

You can specify an object name more than once. If it is intended to
be a different instance of the same object, simply put a number at the
end. Each distinct number indicates a distinct instance. The numbers
must all be less than 100. (Numbers 100 and greater are reserved for
internal use).

=item *

You can use the converse name of a relationship to make the object name list
read more like regular English.

=back

These requirements do not come up very often, but they can make a big differance.

For example, let us say you are looking for a feature that has a role in a
particular subsystem and also belongs to a particular genome. You can't use

    my $query = $erdb->Get(['Feature', 'HasRoleInSubsystem', 'Subsystem', 'HasFeature', 'Genome'], $filter, \@parms);

because you don't want to join the C<HasFeature> table to the subsystem table.
Instead, you use

    my $query = $erdb->Get("Feature HasRoleInSubsystem Subsystem AND Feature HasFeature Genome", $filter, \@parms);

Now consider a taxonomy hierarchy using the entity C<Class> and the
relationship C<BelongsTo> and say you want to find all subclasses of a
particular class. If you code

    my $query = $erdb->Get("Class BelongsTo Class", 'Class(id) = ?', [$class])

Then the query will only return the particular class, and only if it belongs
to itself. The following query finds every class that belongs to a particular
class.

    my $query = $erdb->Get("Class BelongsTo Class2", 'Class2(id) = ?', [$class]);

This query does the converse. It finds every class belonging to a particular class.

    my $query = $erdb->Get("Class BelongsTo Class2", 'Class(id) = ?', [$class]);

The difference is indicated by the field name used in the filter clause. Because
the first occurrence of C<Class> is specified in the filter rather than the
second occurrence (C<Class2>), the query is anchored on the from-side of the
relationship.

=head4 Filter Clause

The filter clause is an SQL WHERE clause (without the WHERE) to be used to filter
and sort the query. The WHERE clause can be parameterized with parameter markers
(C<?>). Each field used in the WHERE clause must be specified in
L</Standard Field Name Format>. Any parameters specified in the filter clause should
be added to the parameter list as additional parameters. The fields in a filter
clause can come from primary entity relations, relationship relations, or
secondary entity relations; however, all of the entities and relationships
involved must be included in the list of object names on the query. There is
never a default object name for filter clause fields.

The filter clause can also specify a sort order. To do this, simply follow
the filter string with an ORDER BY clause. For example, the following filter
string gets all genomes for a particular genus and sorts them by species name.

    "Genome(genus) = ? ORDER BY Genome(species)"

Note that the case is important. Only an uppercase "ORDER BY" with a single
space will be processed. The idea is to make it less likely to find the verb by
accident.

The rules for field references in a sort order are the same as those for field
references in the filter clause in general; however, unpredictable things may
happen if a sort field is from an entity's secondary relation.

Finally, you can limit the number of rows returned by adding a LIMIT clause. The
LIMIT must be the last thing in the filter clause, and it contains only the word
"LIMIT" followed by a positive number. So, for example

    "Genome(genus) = ? ORDER BY Genome(species) LIMIT 10"

will only return the first ten genomes for the specified genus. The ORDER BY
clause is not required. For example, to just get the first 10 genomes in the
B<Genome> table, you could use

    "LIMIT 10"

as your filter clause.

=head4 Parameter List

The parameter list is a reference to a list of parameter values. The parameter
values are substituted for the parameter marks in the filter clause in strict
left-to-right order.

In the parameter list for a filter clause, you must be aware of the proper
data types and perform any necessary conversions manually. This is not normally
a problem. Most of the time, you only query against simple numeric or string
fields, and you only need to convert a string if there's a possibility it has
exotic characters like tabs or new-lines in it. Sometimes, however, this is not
enough.

When you are writing programs to query ERDBtk databases, you can call
L</EncodeField> directly, specifying a field name in the
L</Standard Field Name Format>. The value will be converted as if it
was being stored into a field of the specified type. Alternatively, you
can call L</encode>, specifying a data type name. Both of these techniques
are shown in the example below.

    my $query = $erdb->Get("Genome UsesImage Image",
                           "Image(png) = ? AND Genome(description) = ?",
                           [$erdb->EncodeFIeld('Image(png)', $myImage),
                            ERDBtk::encode(text => $myDescription)]);

You can export the L</encode> method if you expect to be doing this a lot
and don't want to bother with the package name on the call.

    use ERDBtk qw(encode);

    # ... much later ...

    my $query = $erdb->Get("Genome UsesImage Image",
                           "Image(png) = ? AND Genome(description) = ?",
                           [$erdb->EncodeField('Image(png)', $myImage),
                            encode(text => $myDescription)]);

=head2 XML Database Description

=head3 Global Tags

The entire database definition must be inside a B<Database> tag. The display
name of the database is given by the text associated with the B<Title> tag. The
display name is only used in the automated documentation. The entities and
relationships are listed inside the B<Entities> and B<Relationships> tags,
respectively. There is also a C<Shapes> tag that contains additional shapes to
display on the database diagram, and an C<Issues> tag that describes general
things that need to be remembered. These last two are completely optional.

    <Database>
        <Title>... display title here...</Title>
        <Issues>
            ... comments here ...
        </Issues>
        <Regions>
            ... region definitions here ...
        </Regions>
        <Entities>
            ... entity definitions here ...
        </Entities>
        <Relationships>
            ... relationship definitions here ...
        </Relationships>
        <Shapes>
           ... shape definitions here ...
        </Shapes>
    </Database>

=head3 Notes and Asides

Entities, relationships, shapes, indexes, and fields all allow text tags called
B<Notes> and B<Asides>. Both these tags contain comments that appear when the
database documentation is generated. In addition, the text inside the B<Notes>
tag will be shown as a tooltip when mousing over the diagram.

The following special codes allow a limited rich text capability in Notes and
Asides.

[b]...[/b]: Bold text

[i]...[/i]: Italics

[p]...[/p]: Paragraph

[link I<href>]...[/link]: Hyperlink to the URL I<href>

[list]...[*]...[*]...[/list]: Bullet list, with B<[*]> separating list elements.

=head3 Fields

Both entities and relationships have fields described by B<Field> tags. A
B<Field> tag can have B<Notes> associated with it. The complete set of B<Field>
tags for an object mus be inside B<Fields> tags.

    <Entity ... >
        <Fields>
            ... Field tags ...
        </Fields>
    </Entity>

The attributes for the B<Field> tag are as follows.

=over 4

=item name

Name of the field. The field name should contain only letters, digits, and
hyphens (C<->), and the first character should be a letter. Most underlying
databases are case-insensitive with the respect to field names, so a best
practice is to use lower-case letters only. Finally, the name
C<search-relevance> has special meaning for full-text searches and should not be
used as a field name.

=item type

Data type of the field.

=item relation

Name of the relation containing the field. This should only be specified for
entity fields. The ERDBtk system does not support optional fields or
multi-occurring fields in the primary relation of an entity. Instead, they are
put into secondary relations. So, for example, in the C<Genome> entity, the
C<group-name> field indicates a special grouping used to select a subset of the
genomes. A given genome may not be in any groups or may be in multiple groups.
Therefore, C<group-name> specifies a relation value. The relation name specified
must be a valid table name. By convention, it is usually the entity name
followed by a qualifying word (e.g. C<GenomeGroup>). In an entity, the fields
without a relation attribute are said to belong to the I<primary relation>. This
relation has the same name as the entity itself.

=item special

This attribute allows the subclass to assign special meaning for certain fields.
The interpretation is up to the subclass itself. Currently, only entity fields
can have this attribute.

=item default

This attribute specifies the default field value to be used while loading. The
default value is used if no value is specified in an L</InsertObject> call or in
the L<ERDBtkLoadGroup/Put> call that generates the load file. If no default is
specified, then the field is required and must have a value specified in the
call.

The default value is specified as a string, so it must be in an encoded
form.

=item null

If C<1>, this attribute indicates that the field can have a null value. The
default is C<0>.

=back

=head3 Indexes

An entity can have multiple alternate indexes associated with it. The fields in
an index must all be from the same relation. The alternate indexes assist in
searching on fields other than the entity ID. A relationship has at least two
indexes-- a I<to-index> and a I<from-index> that order the results when crossing
the relationship. For example, in the relationship C<HasContig> from C<Genome>
to C<Contig>, the from-index would order the contigs of a ganome, and the
to-index would order the genomes of a contig. In addition, it can have zero or
more alternate indexes. A relationship's index can only specify fields in the
relationship.

The alternate indexes for an entity or relationship are listed inside the
B<Indexes> tag. The from-index of a relationship is specified using the
B<FromIndex> tag; the to-index is specified using the B<ToIndex> tag.

Be aware of the fact that in some versions of MySQL, the maximum size of an
index key is 1000 bytes. This means at most four normal-sized strings.

The B<Index> tag has one optional attribute.

=over 4

=item unique

If C<1>, then the index is unique. The default is C<0> (a non-unique index).

=back

Each index can contain a B<Notes> tag. In addition, it will have an
B<IndexFields> tag containing the B<IndexField> tags. The B<IndexField>
tags specify, in order, the fields used in the index. The attributes of an
B<IndexField> tag are as follows.

=over 4

=item name

Name of the field.

=item order

Sort order of the field-- C<ascending> or C<descending>.

=back

The B<FromIndex>, B<ToIndex> and B<Index> tags can have a B<unique> attribute.
If specified, the index will be generated as a unique index. The B<ToIndex>
for a one-to-many relationship is always unique.

=head3 Regions

A large database may be too big to fit comfortably on a single page. When this
happens, you have the option of dividing the diagram into regions that are shown
one at a time. When regions are present, a combo box will appear on the diagram
allowing the user to select which region to show. Each entity, relationship, or
shape can have multiple B<RegionInfo> tags describing how it should be displayed
when a particular region is selected. The regions themselves are described by
a B<Region> tag with a single attribute-- B<name>-- that indicates the region
name. The tag can be empty, or can contain C<Notes> elements that provide useful
documentation.

=over 4

=item name

Name of the region.

=back

=head3 Diagram

The diagram tag allows you to specify options for generating a diagram. If the
tag is present, then it will be used to configure diagram display in the
documentation widget (see L<ERDBtk::PDocPage>). the tag has the following
attributes. It should not have any content; that is, it is not a container
tag.

=over 4

=item width

Width for the diagram, in pixels. The default is 750.

=item height

Height for the diagram, in pixels. The default is 800.

=item ratio

Ratio of shape height to width. The default is 0.62.

=item size

Width in pixels for each shape.

=item nonoise

If set to 1, there will be a white background instead of an NMPDR noise background.

=item editable

If set to 1, a dropdown box and buttons will appear that allow you to edit the diagram,
download your changes, and make it pretty for printing.

=item fontSize

Maximum font size to use, in points. The default is 16.

=item download

URL of the CGI script that downloads the diagram XML to the user's computer. The XML text
will be sent via the C<data> parameter and the default file name via the C<name>
parameter.

=item margin

Margin between adjacent shapes, in pixels. The default is 10.

=back

=head3 DisplayInfo

The B<DisplayInfo> tag is used to describe how an entity, relationship, or shape
should be displayed when the XML file is used to generate an interactive
diagram. A B<DisplayInfo> can have no elements, or it can have multiple
B<Region> elements inside. The permissible attributes are as follows.

=over 4

=item link

URL to which the user should be sent when clicking on the shape. For entities
and relationships, this defaults to the most likely location for the object
description in the generated documentation.

=item theme

The themes are C<black>, C<blue>, C<brown>, C<cyan>, C<gray>, C<green>,
C<ivory>, C<navy>, C<purple>, C<red>, and C<violet>. These indicate the color to
be used for the displayed object. The default is C<gray>.

=item col

The number of the column in which the object should be displayed. Fractional
column numbers are legal, though it's best to round to a multiple of 0.5. Thus,
a column of C<4.5> would be centered between columns 4 and 5.

=item row

The number of the row in which the object should be displayed. Fractional row
numbers are allowed in the same manner as for columns.

=item connected

If C<1>, the object is visibly connected by lines to the other objects
identified in the C<from> and C<to> attributes. This value is ignored for
entities, which never have C<from> or C<to>.

=item caption

Caption to be displayed on the object. If omitted, it defaults to the object's
name. You may use spaces and C<\n> codes to make the caption prettier.

=item fixed

If C<1>, then the C<row> and C<col> attributes are used to position the
object, even if it has C<from> and C<to> attributes. Otherwise, the object is
placed in the midpoint between the C<from> and C<to> shapes.

=back

=head3 RegionInfo

For large diagrams, the B<DisplayInfo> tag may have one or more B<RegionInfo>
elements inside, each belonging to one or more named regions. (The named regions
are desribed by the B<Region> tag.) The diagrammer will create a drop-down box
that can be used to choose which region should be displayed. Each region tag has
a C<name> attribute indicating the region to which it belongs, plus any of the
attributes allowed on the B<DisplayInfo> tag. The name indicates the name of a
region in which the parent object should be displayed. The other attributes
override the corresponding attributes in the B<DisplayInfo> parent. An object
with no Region tags present will be displayed in all regions. There is a default
region with no name that consists only of objects displayed in all regions. An
object with no B<DisplayInfo> tag at all will not be displayed in any region.

=head3 Object and Field Names

By convention entity and relationship names use capital casing (e.g. C<Genome>
or C<HasRegionsIn>. Most underlying databases, however, are aggressively
case-insensitive with respect to relation names, converting them internally to
all-upper case or all-lower case.

If syntax or parsing errors occur when you try to load or use an ERDBtk database,
the most likely reason is that one of your objects has an SQL reserved word as
its name. The list of SQL reserved words keeps increasing; however, most are
unlikely to show up as a noun or declarative verb phrase. The exceptions are
C<Group>, C<User>, C<Table>, C<Index>, C<Object>, C<Date>, C<Number>, C<Update>,
C<Time>, C<Percent>, C<Memo>, C<Order>, and C<Sum>. This problem can crop up in
field names as well.

Every entity has a field called C<id> that acts as its primary key. Every
relationship has fields called C<from-link> and C<to-link> that contain copies
of the relevant entity IDs. These are essentially ERDBtk's reserved words, and
should not be used for user-defined field names.

=head3 Issues

Issues are comments displayed at the top of the database documentation. They
have no effect on the database or the diagram. The C<Issue> tag is a text tag
with no attributes.

=head3 Entities

An entity is described by the B<Entity> tag. The entity can contain B<Notes> and
B<Asides>, an optional B<DisplayInfo> tag, an B<Indexes> tag containing one or
more secondary indexes, and a B<Fields> tag containing one or more fields. The
attributes of the B<Entity> tag are as follows.

=over 4

=item name

Name of the entity. The entity name, by convention, uses capital casing (e.g.
C<Genome> or C<GroupBlock>) and should be a noun or noun phrase.

=item keyType

Data type of the primary key. The primary key is always named C<id>.

=item autocounter

A value of C<1> means that the ID numbers must be requested from data in the
system C<_id> table. The key must be of type C<counter>.

=back

=head3 Relationships

A relationship is described by the B<Relationship> tag. Within a relationship,
there can be B<DisplayInfo>, B<Notes> and B<Asides> tags, a B<Fields> tag
containing the intersection data fields, a B<FromIndex> tag containing the
index used to cross the relationship in the forward direction, a B<ToIndex> tag
containing the index used to cross the relationship in reverse, and an
C<Indexes> tag containing the alternate indexes.

The B<Relationship> tag has the following attributes.

=over 4

=item name

Name of the relationship. The relationship name, by convention, uses capital
casing (e.g. C<ContainsRegionIn> or C<HasContig>), and should be a declarative
verb phrase, designed to fit between the from-entity and the to-entity (e.g.
Block C<ContainsRegionIn> Genome).

=item from

Name of the entity from which the relationship starts.

=item to

Name of the entity to which the relationship proceeds.

=item arity

Relationship type: C<1M> for one-to-many and C<MM> for many-to-many.

=item converse

A name to be used when travelling backward through the relationship. This
value can be used in place of the real relationship name to make queries
more readable.

=item loose

If TRUE (C<1>), then deletion of an entity instance on the B<from> side
will NOT cause deletion of the connected entity instances on the B<to>
side. All many-to-many relationships are automatically loose. A one-to-many
relationship is generally not loose, but specifying this attribute can make
it so.

item embedded

If TRUE (C<1>), the relationship is embedded in the entity described in the
Cto> attribute. The relationship's C<from-link> and all its attributes are
fields in the entity, while the entity's C<id> field serves as the relationship's
C<to-link>. In this case, the relationship can only have a C<FromIndex>. It cannot
have any alternate indexes.

=back

=head3 Shapes

Shapes are objects drawn on the database diagram that do not physically exist
in the database. Entities are always drawn as rectangles and relationships are
always drawn as diamonds, but a shape can be either of those, an arrow, a
bidirectional arrow, or an oval. The B<Shape> tag can contain B<Notes>,
B<Asides>, and B<DisplayInfo> tags, and has the
following attributes.

=over 4

=item type

Type of shape: C<arrow> for an arrow, C<biarrow> for a bidirectional arrow,
C<oval> for an ellipse, C<diamond> for a diamond, and C<rectangle> for a
rectangle.

=item from

Object from which this object is oriented. If the shape is an arrow, it
will point toward the from-object.

=item to

Object toward which this object is oriented. If the shape is an arrow, it
will point away from the to-object.

=item name

Name of the shape. This is used by other shapes to identify it in C<from>
and C<to> directives.

=back

=cut

# GLOBALS

# Table of information about our datatypes.
my $TypeTable;

my @StandardTypes = qw(Boolean Char Counter Date Float HashString Integer String Text);

# Table translating arities into natural language.
my %ArityTable = ( '1M' => 'one-to-many',
                   'MM' => 'many-to-many'
                 );

# Options for XML input and output.

my %XmlOptions = (GroupTags =>  { Relationships => 'Relationship',
                                  Entities => 'Entity',
                                  Fields => 'Field',
                                  Indexes => 'Index',
                                  IndexFields => 'IndexField',
                                  Issues => 'Issue',
                                  Regions => 'Region',
                                  Shapes => 'Shape'
                                },
                  KeyAttr =>    { Relationship => 'name',
                                  Entity => 'name',
                                  Field => 'name',
                                  Shape => 'name'
                                },
                  SuppressEmpty => 1,
                 );

my %XmlInOpts  = (
                  ForceArray => [qw(Field Index Issues IndexField Relationship Entity Shape)],
                  ForceContent => 1,
                  NormalizeSpace => 2,
                 );
my %XmlOutOpts = (
                  RootName => 'Database',
                  XMLDecl => 1,
                 );

# Table for flipping between FROM and TO
my %FromTo = (from => 'to', to => 'from');

# Name of metadata table.
use constant METADATA_TABLE => '_metadata';
# Name of ID table.
use constant ID_TABLE => '_ids';

=head2 Special Methods

=head3 new

    my $database = ERDBtk->new($dbh, $metaFileName, %options);

Create a new ERDBtk object.

=over 4

=item dbh

L<DBtk> database object for the target database.

=item metaFileName

Name of the XML file containing the metadata.

=item options

Hash of configuration options.

=back

The supported configuration options are as follows. Options not in this list
will be presumed to be relevant to the subclass and will be ignored.

=over 4

=item demandDriven

If TRUE, the database will be configured for a I<forward-only cursor>. Instead
of caching the query results, the query results will be provided at the rate
in which they are demanded by the client application. This is less stressful
on memory and disk space, but means you cannot have more than one query active
at the same time.

=back

=cut

sub new {
    # Get the parameters.
    my ($class, $dbh, $metaFileName, %options) = @_;
    # Insure we have a type table.
    GetDataTypes();
    # See if we want to use demand-driven flow control for queries.
    if ($options{demandDriven}) {
        $dbh->set_demand_driven(1);
    }
    # Get the quote character.
    my $quote = "";
    if (defined $dbh) {
        $quote = $dbh->quote;
    }
    # Create the object.
    my $self = { _dbh => $dbh,
                 _metaFileName => $metaFileName,
                 _quote => $quote
               };
    # Bless it.
    bless $self, $class;
    # Check for a load directory.
    if ($options{loadDirectory}) {
        $self->{loadDirectory} = $options{loadDirectory};
    }
    # Load the meta-data. (We must be blessed before doing this, because it
    # involves a virtual method.)
    $self->{_metaData} = _LoadMetaData($self, $metaFileName, $options{externalDBD});
    # Return the object.
    return $self;
}

=head3 GetDatabase

    my $erdb = ERDBtk::GetDatabase($name, $dbd, %parms);

Return an ERDBtk object for the named database. It is assumed that the
database name is also the name of a class for connecting to it.

=over 4

=item name

Name of the desired database.

=item dbd

Alternate DBD file to use when processing the database definition.

=item parms

Additional command-line parameters.

=item RETURN

Returns an ERDBtk object for the named database.

=back

=cut

sub GetDatabase {
    # Get the parameters.
    my ($name, $dbd, %parms) = @_;
    # Get access to the database's package.
    require "$name.pm";
    # Plug in the DBD parameter (if any).
    if (defined $dbd) {
        $parms{DBD} = $dbd;
    }
    # Construct the desired object.
    my $retVal = eval("$name->new(%parms)");
    # Fail if we didn't get it.
    Confess("Error connecting to database \"$name\": $@") if $@;
    # Return the result.
    return $retVal;
}

=head3 ParseFieldName

    my ($tableName, $fieldName) = ERDBtk::ParseFieldName($string, $defaultName);

or

    my $normalizedName = ERDBtk::ParseFieldName($string, $defaultName);


Analyze a standard field name to separate the object name part from the
field part.

=over 4

=item string

Standard field name string to be parsed.

=item defaultName (optional)

Default object name to be used if the object name is not specified in the
input string.

=item RETURN

In list context, returns the table name followed by the base field name. In
scalar context, returns the field name in a normalized L</Standard Field Name Format>,
with an object name present. If the parse fails, will return an undefined value.

=back

=cut

sub ParseFieldName {
    # Get the parameters.
    my ($string, $defaultName) = @_;
    # Declare the return values.
    my ($tableName, $fieldName);
    # Get a copy of the input string,
    my $realString = $string;
    # Parse the input string.
    if ($realString =~ /^(\w+)\(([\w\-]+)\)$/) {
        # It's a standard name. Return the pieces.
        ($tableName, $fieldName) = ($1, $2);
    } elsif ($realString =~ /^[\w\-]+$/ && defined $defaultName) {
        # It's a plain name, and we have a default table name.
        ($tableName, $fieldName) = ($defaultName, $realString);
    }
    # Return the results.
    if (wantarray()) {
        return ($tableName, $fieldName);
    } elsif (! defined $tableName) {
        return undef;
    } else {
        return "$tableName($fieldName)";
    }
}

=head3 CountParameterMarks

    my $count = ERDBtk::CountParameterMarks($filterString);

Return the number of parameter marks in the specified filter string.

=over 4

=item filterString

ERDBtk filter clause to examine.

=item RETURN

Returns the number of parameter marks in the specified filter clause.

=back

=cut

sub CountParameterMarks {
    # Get the parameters.
    my ($filterString) = @_;
    # Declare the return variable.
    my $retVal = 0;
    # Get a safety copy of the filter string.
    my $filterCopy = $filterString;
    # Remove all escaped quotes.
    $filterCopy =~ s/\\'//g;
    # Remove all quoted strings.
    $filterCopy =~ s/'[^']*'//g;
    # Count the question marks.
    while ($filterCopy =~ /\?/g) {
        $retVal++
    }
    # Return the result.
    return $retVal;
}


=head2 Query Methods

=head3 GetEntity

    my $entityObject = $erdb->GetEntity($entityType, $ID);

Return an object describing the entity instance with a specified ID.

=over 4

=item entityType

Entity type name.

=item ID

ID of the desired entity.

=item RETURN

Returns a L<ERDBtk::Object> object representing the desired entity instance, or
an undefined value if no instance is found with the specified key.

=back

=cut

sub GetEntity {
    # Get the parameters.
    my ($self, $entityType, $ID) = @_;
    # Encode the ID value.
    my $coded = $self->EncodeField("$entityType(id)", $ID);
    # Create a query.
    my $query = $self->Get($entityType, "$entityType(id) = ?", [$coded]);
    # Get the first (and only) object.
    my $retVal = $query->Fetch();
    # Return the result.
    return $retVal;
}

=head3 GetChoices

    my @values = $erdb->GetChoices($entityName, $fieldName);

Return a list of all the values for the specified field that are represented in
the specified entity.

Note that if the field is not indexed, then this will be a very slow operation.

=over 4

=item entityName

Name of an entity in the database.

=item fieldName

Name of a field belonging to the entity in L</Standard Field Name Format>.

=item RETURN

Returns a list of the distinct values for the specified field in the database.

=back

=cut

sub GetChoices {
    # Get the parameters.
    my ($self, $entityName, $fieldName) = @_;
    # Get the entity data structure.
    my $entityData = $self->_GetStructure($entityName);
    # Get the field descriptor.
    my $fieldData = $self->_FindField($fieldName, $entityName);
    # Get the name of the relation containing the field.
    my $relation = $fieldData->{relation};
    # Fix up the field name.
    my $realName = _FixName($fieldData->{name});
    # Get the field type.
    my $type = $fieldData->{type};
    # Get the database handle.
    my $dbh = $self->{_dbh};
    # Get the quote character.
    my $q = $self->q;
    # Query the database.
    my $results = $dbh->SQL("SELECT DISTINCT $q$realName$q FROM $q$relation$q");
    # Clean the results. They are stored as a list of lists,
    # and we just want the one list. Also, we want to decode the values.
    my @retVal = sort map { $TypeTable->{$type}->decode($_->[0]) } @{$results};
    # Return the result.
    return @retVal;
}

=head3 GetEntityValues

    my @values = $erdb->GetEntityValues($entityType, $ID, \@fields);

Return a list of values from a specified entity instance. If the entity instance
does not exist, an empty list is returned.

=over 4

=item entityType

Entity type name.

=item ID

ID of the desired entity.

=item fields

List of field names in L</Standard_Field_Name_Format>.

=item RETURN

Returns a flattened list of the values of the specified fields for the specified entity.

=back

=cut

sub GetEntityValues {
    # Get the parameters.
    my ($self, $entityType, $ID, $fields) = @_;
    # Get the specified entity.
    my ($entity) = $self->GetAll($entityType, "$entityType(id) = ?", [$ID], $fields);
    # Declare the return list.
    my @retVal = ();
    # If we found the entity, push the values into the return list.
    if ($entity) {
        push @retVal, @$entity;
    }
    # Return the result.
    return @retVal;
}

=head3 GetAll

    my @list = $erdb->GetAll(\@objectNames, $filterClause, \@parameters, \@fields, $count);

Return a list of values taken from the objects returned by a query. The first
three parameters correspond to the parameters of the L</Get> method. The final
parameter is a list of the fields desired from each record found by the query
in L</Standard Field Name Format>. The default object name is the first one in the
object name list.

The list returned will be a list of lists. Each element of the list will contain
the values returned for the fields specified in the fourth parameter. If one of the
fields specified returns multiple values, they are flattened in with the rest. For
example, the following call will return a list of the features in a particular
spreadsheet cell, and each feature will be represented by a list containing the
feature ID followed by all of its essentiality determinations.

    @query = $erdb->Get('ContainsFeature Feature'], "ContainsFeature(from-link) = ?",
                        [$ssCellID], ['Feature(id)', 'Feature(essential)']);

=over 4

=item objectNames

List containing the names of the entity and relationship objects to be retrieved.
See L</Object Name List>.

=item filterClause

WHERE/ORDER BY clause (without the WHERE) to be used to filter and sort the query.
See L</Filter Clause>.

=item parameterList

List of the parameters to be substituted in for the parameters marks
in the filter clause. See L</Parameter List>.

=item fields

List of the fields to be returned in each element of the list returned, or a
string containing a space-delimited list of field names. The field names should
be in L</Standard Field Name Format>.

=item count

Maximum number of records to return. If omitted or 0, all available records will
be returned.

=item RETURN

Returns a list of list references. Each element of the return list contains the
values for the fields specified in the B<fields> parameter.

=back

=cut
#: Return Type @@;
sub GetAll {
    # Get the parameters.
    my ($self, $objectNames, $filterClause, $parameterList, $fields, $count) = @_;
    # Translate the parameters from a list reference to a list. If the parameter
    # list is a scalar we convert it into a singleton list.
    my @parmList = ();
    if (ref $parameterList eq "ARRAY") {
        @parmList = @{$parameterList};
    } else {
        push @parmList, $parameterList;
    }
    # Insure the counter has a value.
    if (!defined $count) {
        $count = 0;
    }
    # Add the row limit to the filter clause.
    if ($count > 0) {
        $filterClause .= " LIMIT $count";
    }
    # Create the query.
    my $query = $self->Get($objectNames, $filterClause, \@parmList, $fields);
    # Set up a counter of the number of records read.
    my $fetched = 0;
    # Convert the field names to a list if they came in as a string.
    my $fieldList = (ref $fields ? $fields : [split /\s+/, $fields]);
    # Loop through the records returned, extracting the fields. Note that if the
    # counter is non-zero, we stop when the number of records read hits the count.
    my @retVal = ();
    while (($count == 0 || $fetched < $count) && (my $row = $query->Fetch())) {
        my @rowData = $row->Values($fieldList);
        push @retVal, \@rowData;
        $fetched++;
    }
    # Return the resulting list.
    return @retVal;
}


=head3 Exists

    my $found = $erdb->Exists($entityName, $entityID);

Return TRUE if an entity exists, else FALSE.

=over 4

=item entityName

Name of the entity type (e.g. C<Feature>) relevant to the existence check.

=item entityID

ID of the entity instance whose existence is to be checked.

=item RETURN

Returns TRUE if the entity instance exists, else FALSE.

=back

=cut
#: Return Type $;
sub Exists {
    # Get the parameters.
    my ($self, $entityName, $entityID) = @_;
    # Check for the entity instance.
    my $testInstance = $self->GetEntity($entityName, $entityID);
    # Return an existence indicator.
    my $retVal = ($testInstance ? 1 : 0);
    return $retVal;
}

=head3 GetCount

    my $count = $erdb->GetCount(\@objectNames, $filter, \@params);

Return the number of rows found by a specified query. This method would
normally be used to count the records in a single table. For example,

    my $count = $erdb->GetCount('Genome', 'Genome(genus-species) LIKE ?',
                                ['homo %']);

would return the number of genomes for the genus I<homo>. It is conceivable,
however, to use it to return records based on a join. For example,

    my $count = $erdb->GetCount('HasFeature Genome', 'Genome(genus-species) LIKE ?',
                                ['homo %']);

would return the number of features for genomes in the genus I<homo>. Note that
only the rows from the first table are counted. If the above command were

    my $count = $erdb->GetCount('Genome HasFeature', 'Genome(genus-species) LIKE ?',
                                ['homo %']);

it would return the number of genomes, not the number of genome/feature pairs.

=over 4

=item objectNames

Reference to a list of the objects (entities and relationships) included in the
query, or a string containing a space-delimited list of object names. See
L</ObjectNames>.

=item filter

A filter clause for restricting the query. See L</Filter Clause>.

=item params

Reference to a list of the parameter values to be substituted for the parameter
marks in the filter. See L</Parameter List>.

=item RETURN

Returns a count of the number of records in the first table that would satisfy
the query.

=back

=cut

sub GetCount {
    # Get the parameters.
    my ($self, $objectNames, $filter, $params) = @_;
    # Insure the params argument is an array reference if the caller left it
    # off.
    if (! defined($params)) {
        $params = [];
    }
    # Declare the return variable.
    my $retVal;
    # Create an SQL helper for this query path.
    my $sqlHelper = ERDBtk::Helpers::SQLBuilder->new($self, $objectNames);
    # Get the suffix from the filter clause.
    my $suffix = $sqlHelper->SetFilterClause($filter);
    # Compute the field we want to count.
    my $countedField;
    my ($objectName, $baseName) = $sqlHelper->PrimaryInfo();
    if ($self->IsEntity($baseName)) {
        $countedField = "$objectName(id)";
    } else {
        $countedField = "$objectName(to-link)";
    }
    # Compute the field list.
    my $fieldList = $sqlHelper->ComputeFieldList($countedField);
    # Create the SQL command suffix to get the desired records.
    my $command = "SELECT COUNT($fieldList) $suffix";
    # Prepare and execute the command.
    my $sth = $self->_GetStatementHandle($command, $params);
    # Get the count value.
    ($retVal) = $sth->fetchrow_array();
    # Check for a problem.
    if (! defined($retVal)) {
        if ($sth->err) {
            # Here we had an SQL error.
            Confess("Error retrieving row count: " . $sth->errstr());
        } else {
            # Here we have no result.
            Confess("No result attempting to retrieve row count.");
        }
    }
    # Return the result.
    return $retVal;
}


=head3 Get

    my $query = $erdb->Get(\@objectNames, $filterClause, \@params, $fields);

This method returns a query object for entities of a specified type using a
specified filter.

=over 4

=item objectNames

List containing the names of the entity and relationship objects to be retrieved,
or a string containing a space-delimited list of names. See L</Object Name List>.

=item filterClause

WHERE clause (without the WHERE) to be used to filter and sort the query. See
L</Filter Clause>.

=item params

Reference to a list of parameter values to be substituted into the filter
clause. See L</Parameter List>.

=item fields

A list of fields in L</Standard Field Name Format>. Only the fields in the
list will be retrieved from the database.

=item RETURN

Returns an L</ERDBtk::Query> object that can be used to iterate through all of the
results.

=back

=cut

sub Get {
    # Get the parameters.
    my ($self, $objectNames, $filterClause, $params, $fields) = @_;
    # Compute the SQL components of the query.
    my $sqlHelper = ERDBtk::Helpers::SQLBuilder->new($self, $objectNames);
    my $suffix = $sqlHelper->SetFilterClause($filterClause);
    my $fieldList = $sqlHelper->ComputeFieldList($fields);
    # Create the query.
    my $command = "SELECT $fieldList $suffix";
    my $sth = $self->_GetStatementHandle($command, $params);
    # Return the statement object.
    my $retVal = ERDBtk::Query::_new($self, $sth, $sqlHelper);
    return $retVal;
}

=head3 GetFlat

    my @list = $erdb->GetFlat(\@objectNames, $filterClause, \@parameterList, $field);

This is a variation of L</GetAll> that asks for only a single field per record
and returns a single flattened list.

=over 4

=item objectNames

List containing the names of the entity and relationship objects to be retrieved,
or a string containing a space-delimited list of names. See L</Object_Name_List>.

=item filterClause

WHERE clause (without the WHERE) to be used to filter and sort the query. See
L</Filter Clause>.

=item params

Reference to a list of parameter values to be substituted into the filter
clause. See L</Parameter List>.

=item field

Name of the field to be used to get the elements of the list returned. The
default object name for this context is the first object name specified.

=item RETURN

Returns a list of values.

=back

=cut

sub GetFlat {
    # Get the parameters.
    my ($self, $objectNames, $filterClause, $parameterList, $field) = @_;
    # Construct the query.
    my $query = $self->Get($objectNames, $filterClause, $parameterList, $field);
    # Create the result list.
    my @retVal = ();
    # Loop through the records, adding the field values found to the result list.
    while (my $row = $query->Fetch()) {
        push @retVal, $row->Value($field);
    }
    # Return the list created.
    return @retVal;
}

=head3 IsUsed

    my $flag = $erdb->IsUsed($relationName);

Returns TRUE if the specified relation contains any records, else FALSE.

=over 4

=item relationName

Name of the relation to check.

=item RETURN

Returns the number of records in the relation, which will be TRUE if the
relation is nonempty and FALSE otherwise.

=back

=cut

sub IsUsed {
    # Get the parameters.
    my ($self, $relationName) = @_;
    # Get the data base handle and quote character.
    my $q = $self->q;
    my $dbh = $self->{_dbh};
    # Construct a query to count the records in the relation.
    my $cmd = "SELECT COUNT(*) FROM $q$relationName$q";
    my $results = $dbh->SQL($cmd);
    # We'll put the count in here.
    my $retVal = 0;
    if ($results && scalar @$results) {
        $retVal = $results->[0][0];
    }
    # Return the count.
    return $retVal;
}

=head2 Documentation and Metadata Methods

=head3 q

    my $q = $erdb->q;

Return the quote character used to protect SQL identifiers.

=cut

sub q {
    return $_[0]->{_quote};
}


=head3 ComputeFieldTable

    my ($header, $rows) = ERDBtk::ComputeFieldTable($wiki, $name, $fieldData);

Generate the header and rows of a field table for an entity or
relationship. The field table describes each field in the specified
object.

=over 4

=item wiki

L<WikiTools> object (or equivalent) for rendering HTML or markup.

=item name

Name of the object whose field table is being generated.

=item fieldData

Field structure of the specified entity or relationship.

=item embedded

TRUE if this is an embedded relationship, else FALSE.

=item RETURN

Returns a reference to a list of the labels for the header row and
a reference to a list of lists representing the table cells.

=back

=cut

sub ComputeFieldTable {
    # Get the parameters.
    my ($wiki, $name, $fieldData, $embedded) = @_;
    # We need to sort the fields. First comes the ID, then the
    # primary fields and the secondary fields.
    my %sorter;
    for my $field (keys %$fieldData) {
        # Get the field's descriptor.
        my $fieldInfo = $fieldData->{$field};
        # Determine whether or not we have a primary field.
        my $primary;
        if ($field eq 'id') {
            $primary = 'A';
        } elsif ($embedded || $fieldInfo->{relation} eq $name) {
            $primary = 'B';
        } else {
            $primary = 'C';
        }
        # Form the sort key from the flag and the name.
        $sorter{$field} = "$primary$field";
    }
    # Create the header descriptor for the table.
    my @header = qw(Name Type Notes);
    # We'll stash the rows in here.
    my @rows;
    # Loop through the fields in their proper order.
    for my $field (StringUtils::SortByValue(\%sorter)) {
        # Get the field's descriptor.
        my $fieldInfo = $fieldData->{$field};
        # Format the type.
        my $type = "$fieldInfo->{type}";
        if ($fieldInfo->{null}) {
            $type .= " (nullable)";
        }
        # Secondary fields have "C" as the first letter in
        # the sort value.
        if ($sorter{$field} =~ /^C/) {
            $type .= " array";
        }
        # Format its table row.
        push @rows, [$field, $type, ObjectNotes($fieldInfo, $wiki)];
    }
    # Return the results.
    return (\@header, \@rows);
}

=head3 FindEntity

    my $objectData = $erdb->FindEntity($name);

Return the structural descriptor of the specified entity, or an undefined
value if the entity does not exist.

=over 4

=item name

Name of the desired entity.

=item RETURN

Returns the definition structure for the specified entity, or C<undef>
if the named entity does not exist.

=back

=cut

sub FindEntity {
    # Get the parameters.
    my ($self, $name) = @_;
    # Return the result.
    return $self->_FindObject(Entities => $name);
}

=head3 FindRelationship

    my $objectData = $erdb->FindRelationship($name);

Return the structural descriptor of the specified relationship, or an undefined
value if the relationship does not exist. The relationship name can be a regular
name or a converse.

=over 4

=item name

Name of the desired relationship.

=item RETURN

Returns the definition structure for the specified relationship, or C<undef>
if the named relationship does not exist.

=back

=cut

sub FindRelationship {
    # Get the parameters.
    my ($self, $name) = @_;
    # Check for a converse.
    my $obverse = $self->{_metaData}{ConverseTable}{$name} // $name;
    # Return the result.
    return $self->_FindObject(Relationships => $obverse);
}

=head3 ComputeTargetEntity

    my $targetEntity = $erdb->ComputeTargetEntity($relationshipName);

Return the target entity of a relationship. If the relationship's true
name is specified, this is the source (from) entity. If its converse
name is specified, this is the target (to) entity. The returned name is
the one expected to follow the relationship name in an object name string.

=over 4

=item relationshipName

The name of the relationship to be used to identify the target entity.

=item RETURN

Returns the name of the entity that would be found after crossing
the relationship in the direction indicated by the chosen relationship
name. If the relationship name is invalid, an undefined value will be
returned.

=back

=cut

sub ComputeTargetEntity {
    # Get the parameters.
    my ($self, $relationshipName) = @_;
    # Declare the return variable.
    my $retVal;
    # Check for a converse.
    my $converse = $self->{_metaData}{ConverseTable}{$relationshipName};
    my $obverse = $converse // $relationshipName;
    # Get the relationship descriptor.
    my $relData = $self->_FindObject(Relationships => $obverse);
    # Only proceed if it exists.
    if ($relData) {
        # Compute the appropriate entity name.
        if ($converse) {
            $retVal = $relData->{from};
        } else {
            $retVal = $relData->{to};
        }
    }
    # Return the entity name found.
    return $retVal;
}

=head3 FindShape

    my $objectData = $erdb->FindShape($name);

Return the structural descriptor of the specified shape, or an undefined
value if the shape does not exist.

=over 4

=item name

Name of the desired shape.

=item RETURN

Returns the definition structure for the specified shape, or C<undef>
if the named shape does not exist.

=back

=cut

sub FindShape {
    # Get the parameters.
    my ($self, $name) = @_;
    # Return the result.
    return $self->_FindObject(Shapes => $name);
}

=head3 GetObjectsTable

    my $objectHash = $erdb->GetObjectsTable($type);

Return the metadata hash of objects of the specified type-- entity,
relationship, or shape.

=over 4

=item type

Type of object desired-- C<entity>, C<relationship>, or C<shape>.

=item RETURN

Returns a reference to a hash containing all metadata for database
objects of the specified type. The hash maps object names to object
descriptors. The descriptors represent a cleaned and normalized
version of the definition XML. Specifically, all of the implied
defaults are filled in.

=back

=cut

sub GetObjectsTable {
    # Get the parameters.
    my ($self, $type) = @_;
    # Return the result.
    return $self->{_metaData}->{ERDBtk::Plurals($type)};
}

=head3 Plurals

    my $plural = ERDBtk::Plurals($singular);

Return the plural form of the specified object type (entity,
relationship, or shape). This is extremely useful in generating
documentation.

=over 4

=item singular

Singular form of the specified object type.

=item RETURN

Plural form of the specified object type, in capital case.

=back

=cut

sub Plurals {
    # Get the parameters.
    my ($singular) = @_;
    # Convert to capital case.
    my $retVal = ucfirst $singular;
    # Handle a "y" at the end.
    $retVal =~ s/y$/ie/;
    # Add the "s".
    $retVal .= "s";
    # Return the result.
    return $retVal;
}

=head3 ReadMetaXML

    my $rawMetaData = ERDBtk::ReadDBD($fileName);

This method reads a raw database definition XML file and returns it.
Normally, the metadata used by the ERDBtk system has been processed and
modified to make it easier to load and retrieve the data; however,
this method can be used to get the data in its raw form.

=over 4

=item fileName

Name of the XML file to read.

=item RETURN

Returns a hash reference containing the raw XML data from the specified file.

=back

=cut

sub ReadMetaXML {
    # Get the parameters.
    my ($fileName) = @_;
    # Read the XML.
    my $retVal = XML::Simple::XMLin($fileName, %XmlOptions, %XmlInOpts);
    # Return the result.
    return $retVal;
}

=head3 FieldType

    my $type = $erdb->FieldType($string, $defaultName);

Return the L<ERDBtk::Type> object for the specified field.

=over 4

=item string

Field name string to be parsed. See L</Standard Field Name Format>.

=item defaultName (optional)

Default object name to be used if the object name is not specified in the
input string.

=item RETURN

Return the type object for the field's type.

=back

=cut

sub FieldType {
    # Get the parameters.
    my ($self, $string, $defaultName) = @_;
    # Get the field descriptor.
    my $fieldData = $self->_FindField($string, $defaultName);
    # Compute the type.
    my $retVal = $TypeTable->{$fieldData->{type}};
    # Return the result.
    return $retVal;
}

=head3 IsSecondary

    my $type = $erdb->IsSecondary($string, $defaultName);

Return TRUE if the specified field is in a secondary relation, else
FALSE.

=over 4

=item string

Field name string to be parsed. See L</Standard Field Name Format>.

=item defaultName (optional)

Default object name to be used if the object name is not specified in the
input string.

=item RETURN

Returns TRUE if the specified field is in a secondary relation, else FALSE.

=back

=cut

sub IsSecondary {
    # Get the parameters.
    my ($self, $string, $defaultName) = @_;
    # Get the field's name and object.
    my ($objName, $fieldName) = ERDBtk::ParseFieldName($string, $defaultName);
    # This will be the return value.
    my $retVal;
    # Only entities can have secondary fields.
    if ($self->IsEntity($objName)) {
        # Retrieve its descriptor from the metadata.
        my $fieldData = $self->_FindField($fieldName, $objName);
        # Compare the table name to the object name.
        $retVal = ($fieldData->{relation} ne $objName);
    }
    # Return the result.
    return $retVal;
}

=head3 FindRelation

    my $relData = $erdb->FindRelation($relationName);

Return the descriptor for the specified relation.

=over 4

=item relationName

Name of the relation whose descriptor is to be returned.

=item RETURN

Returns the object that describes the relation's indexes and fields, or C<undef> if
the relation does not eixst.

=back

=cut
sub FindRelation {
    # Get the parameters.
    my ($self, $relationName) = @_;
    # Get the relation's structure from the master relation table in the
    # metadata structure.
    my $metaData = $self->{_metaData};
    my $retVal = $metaData->{RelationTable}{$relationName};
    # Return it to the caller.
    return $retVal;
}

=head3 GetRelationOwner

    my $objectName = $erdb->GetRelationOwner($relationName);

Return the name of the entity or relationship that owns the specified relation.

=over 4

=item relationName

Relation of interest.

=item RETURN

Returns the name of the owning entity or relationship.

=back

=cut

sub GetRelationOwner {
    # Get the parameters.
    my ($self, $relationName) = @_;
    # Declare the return variable.
    my $retVal;
    # Get the relation's descriptor.
    my $descriptor = $self->FindRelation($relationName);
    if (! $descriptor) {
        Confess("Relation name $relationName not found in database.");
    } else {
        # Get the owner name.
        $retVal = $descriptor->{owner};
    }
    # Return the owner name found.
    return $retVal;
}

=head3 GetSecondaryRelations

    my @secondaries = $erdb->GetSecondaryRelations($objectName);

Get the list of secondary relations for the specified object. There are
none if it is a relationship. There may be one or more for an entity.
These are the names of the relations containing the secondary fields.

=over 4

=item objectName

Name of the relevant entity or relationship.

=item RETURN

Returns a list of the names of the secondary relations.

=back

=cut

sub GetSecondaryRelations {
    # Get the parameters.
    my ($self, $objectName) = @_;
    # Declare the return variable,
    my @retVal;
    # Look for the object in the entity table.
    my $descriptor = $self->FindEntity($objectName);
    # Only proceed if we found it. If we didn't, there can't
    # be any secondaries.
    if ($descriptor) {
        # Get the list of relation names, removing the primary.
        @retVal = grep { $_ ne $objectName } keys %{$descriptor->{Relations}};
    }
    # Return the list found.
    return @retVal;
}


=head3 GetRelationshipEntities

    my ($fromEntity, $toEntity) = $erdb->GetRelationshipEntities($relationshipName);

Return the names of the source and target entities for a relationship. If
the specified name is not a relationship, an empty list is returned.

=over 4

=item relationshipName

Name of the relevant relationship.

=item RETURN

Returns a two-element list. The first element is the name of the relationship's
from-entity, and the second is the name of the to-entity. If the specified name
is not for a relationship, both elements are undefined.

=back

=cut

sub GetRelationshipEntities {
    # Get the parameters.
    my ($self, $relationshipName) = @_;
    # Declare the return variable.
    my @retVal = (undef, undef);
    # Try to find the caller-specified name in the relationship table.
    my $relationships = $self->{_metaData}->{Relationships};
    if (exists $relationships->{$relationshipName}) {
        # We found it. Return the from and to.
        @retVal = map { $relationships->{$relationshipName}->{$_} } qw(from to);
    }
    # Return the results.
    return @retVal;
}


=head3 ValidateFieldName

    my $okFlag = ERDBtk::ValidateFieldName($fieldName);

Return TRUE if the specified field name is valid, else FALSE. Valid field names must
be hyphenated words subject to certain restrictions.

=over 4

=item fieldName

Field name to be validated.

=item RETURN

Returns TRUE if the field name is valid, else FALSE.

=back

=cut

sub ValidateFieldName {
    # Get the parameters.
    my ($fieldName) = @_;
    # Declare the return variable. The field name is valid until we hear
    # differently.
    my $retVal = 1;
    # Look for bad stuff in the name.
    if ($fieldName =~ /--/) {
        # Here we have a doubled minus sign.
        $retVal = 0;
    } elsif ($fieldName !~ /^[A-Za-z]/) {
        # Here the field name is missing the initial letter.
        $retVal = 0;
    } else {
        # Strip out the minus signs. Everything remaining must be a letter
        # or digit.
        my $strippedName = $fieldName;
        $strippedName =~ s/-//g;
        if ($strippedName !~ /^([a-z]|\d)+$/i) {
            $retVal = 0;
        }
    }
    # Return the result.
    return $retVal;
}

=head3 GetFieldTable

    my $fieldHash = $self->GetFieldTable($objectnName);

Get the field structure for a specified entity or relationship.

=over 4

=item objectName

Name of the desired entity or relationship.

=item RETURN

The table containing the field descriptors for the specified object.

=back

=cut

sub GetFieldTable {
    # Get the parameters.
    my ($self, $objectName) = @_;
    # Get the descriptor from the metadata.
    my $objectData = $self->_GetStructure($objectName);
    # Return the object's field table.
    return $objectData->{Fields};
}

=head3 EstimateRowSize

    my $rowSize = $erdb->EstimateRowSize($relName);

Estimate the row size of the specified relation. The estimated row size is
computed by adding up the average length for each data type.

=over 4

=item relName

Name of the relation whose estimated row size is desired.

=item RETURN

Returns an estimate of the row size for the specified relation.

=back

=cut
#: Return Type $;
sub EstimateRowSize {
    # Get the parameters.
    my ($self, $relName) = @_;
    # Declare the return variable.
    my $retVal = 0;
    # Find the relation descriptor.
    my $relation = $self->FindRelation($relName);
    # Get the list of fields.
    for my $fieldData (@{$relation->{Fields}}) {
        # Get the field type and add its length.
        my $fieldLen = $TypeTable->{$fieldData->{type}}->averageLength();
        $retVal += $fieldLen;
    }
    # Return the result.
    return $retVal;
}

=head3 SortNeeded

    my $parms = $erdb->SortNeeded($relationName);

Return the pipe command for the sort that should be applied to the specified
relation when creating the load file.

For example, if the load file should be sorted ascending by the first
field, this method would return

    sort -k1 -t"\t"

If the first field is numeric, the method would return

    sort -k1n -t"\t"

=over 4

=item relationName

Name of the relation to be examined. This could be an entity name, a relationship
name, or the name of a secondary entity relation.

=item RETURN

Returns the sort command to use for sorting the relation, suitable for piping.

=back

=cut

sub SortNeeded {
    # Get the parameters.
    my ($self, $relationName) = @_;
    # Declare a descriptor to hold the names of the key fields.
    my @keyNames = ();
    # Get the relation structure.
    my $relationData = $self->FindRelation($relationName);
    # Get the relation's field list.
    my @fields = @{$relationData->{Fields}};
    my @fieldNames = map { $_->{name} } @fields;
    # Find out if the relation is a primary entity relation,
    # a relationship relation, or a secondary entity relation.
    my $entityTable = $self->{_metaData}->{Entities};
    my $relationshipTable = $self->{_metaData}->{Relationships};
    if (exists $entityTable->{$relationName}) {
        # Here we have a primary entity relation. We sort on the ID, and the
        # ID only.
        push @keyNames, "id";
    } elsif (exists $relationshipTable->{$relationName}) {
        # Here we have a relationship. We sort using the FROM index followed by
        # the rest of the fields, in order. First, we get all of the fields in
        # a hash.
        my %fieldsLeft = map { $_ => 1 } @fieldNames;
        # Get the index.
        my $index = $relationData->{Indexes}->{idxFrom};
        # Loop through its fields.
        for my $keySpec (@{$index->{IndexFields}}) {
            # Mark this field as used. The field may have a modifier, so we only
            # take the part up to the first space.
            $keySpec =~ /^(\S+)/;
            $fieldsLeft{$1} = 0;
            push @keyNames, $keySpec;
        }
        # Push the rest of the fields on.
        push @keyNames, grep { $fieldsLeft{$_} } @fieldNames;
    } else {
        # Here we have a secondary entity relation, so we have a sort on the whole
        # record. This essentially gives us a sort on the ID followed by the
        # secondary data field.
        push @keyNames, @fieldNames;
    }
    # Now we parse the key names into sort parameters. First, we prime the return
    # string.
    my $retVal = "sort $ERDBtkExtras::sort_options -u -T\"$ERDBtkExtras::temp\" -t\"\t\" ";
    # Loop through the keys.
    for my $keyData (@keyNames) {
        # Get the key and the ordering.
        my ($keyName, $ordering);
        if ($keyData =~ /^([^ ]+) DESC/) {
            ($keyName, $ordering) = ($1, "descending");
        } else {
            ($keyName, $ordering) = ($keyData, "ascending");
        }
        # Find the key's position and type.
        my $fieldSpec;
        for (my $i = 0; $i <= $#fields && ! $fieldSpec; $i++) {
            my $thisField = $fields[$i];
            if ($thisField->{name} eq $keyName) {
                # Get the sort modifier for this field type. The modifier
                # decides whether we're using a character, numeric, or
                # floating-point sort.
                my $modifier = $TypeTable->{$thisField->{type}}->sortType();
                # If the index is descending for this field, denote we want
                # to reverse the sort order on this field.
                if ($ordering eq 'descending') {
                    $modifier .= "r";
                }
                # Store the position and modifier into the field spec, which
                # will stop the inner loop. Note that the field number is
                # 1-based in the sort command, so we have to increment the
                # index.
                my $realI = $i + 1;
                $fieldSpec = "$realI,$realI$modifier";
            }
        }
        # Add this field to the sort command.
        $retVal .= " -k$fieldSpec";
    }
    # Return the result.
    return $retVal;
}

=head3 SpecialFields

    my %specials = $erdb->SpecialFields($entityName);

Return a hash mapping special fields in the specified entity to the value of their
C<special> attribute. This enables the subclass to get access to the special field
attributes without needed to plumb the internal ERDBtk data structures.

=over 4

=item entityName

Name of the entity whose special fields are desired.

=item RETURN

Returns a hash. The keys of the hash are the special field names, and the values
are the values from each special field's C<special> attribute.

=back

=cut

sub SpecialFields {
    # Get the parameters.
    my ($self, $entityName) = @_;
    # Declare the return variable.
    my %retVal = ();
    # Find the entity's data structure.
    my $entityData = $self->{_metaData}->{Entities}->{$entityName};
    # Loop through its fields, adding each special field to the return hash.
    my $fieldHash = $entityData->{Fields};
    for my $fieldName (keys %{$fieldHash}) {
        my $fieldData = $fieldHash->{$fieldName};
        if (exists $fieldData->{special}) {
            $retVal{$fieldName} = $fieldData->{special};
        }
    }
    # Return the result.
    return %retVal;
}


=head3 GetTableNames

    my @names = $erdb->GetTableNames;

Return a list of the relations required to implement this database.

=cut

sub GetTableNames {
    # Get the parameters.
    my ($self) = @_;
    # Get the relation list from the metadata.
    my $relationTable = $self->{_metaData}->{RelationTable};
    # Return the relation names.
    return keys %{$relationTable};
}

=head3 GetEntityTypes

    my @names = $erdb->GetEntityTypes;

Return a list of the entity type names.

=cut

sub GetEntityTypes {
    # Get the database object.
    my ($self) = @_;
    # Get the entity list from the metadata object.
    my $entityList = $self->{_metaData}->{Entities};
    # Return the list of entity names in alphabetical order.
    return sort keys %{$entityList};
}


=head3 GetRelationshipTypes

    @rels = $erdb->GetRelationshipTypes();

Return a list of all the relationship names in the database.

=cut

sub GetRelationshipTypes {
    # Get the parameters.
    my ($self) = @_;
    # Get the relationship list from the metadata object.
    my $relationshipList = $self->{_metaData}->{Relationships};
    # Return the list of relationship names in alphabetical order.
    return sort keys %$relationshipList;
}


=head3 GetConnectingRelationships

    my @list = $erdb->GetConnectingRelationships($entityName);

Return a list of the relationships connected to the specified entity.

=over 4

=item entityName

Entity whose connected relationships are desired.

=item RETURN

Returns a list of the relationships that originate from the entity.
If the entity is on the I<from> end, it will return the relationship
name. If the entity is on the I<to> end it will return the converse of
the relationship name.

=back

=cut

sub GetConnectingRelationships {
    # Get the parameters.
    my ($self, $entityName) = @_;
    # Declare the return variable.
    my @retVal;
    # Get the relationship list.
    my $relationships = $self->{_metaData}->{Relationships};
    # Find the entity.
    my $entity = $self->{_metaData}->{Entities}->{$entityName};
    # Only proceed if the entity exists.
    if (defined $entity) {
        # Loop through the relationships.
        my @rels = keys %$relationships;
        for my $relationshipName (@rels) {
            my $relationship = $relationships->{$relationshipName};
            if ($relationship->{from} eq $entityName) {
                # Here we have a forward relationship.
                push @retVal, $relationshipName;
            } elsif ($relationship->{to} eq $entityName) {
                # Here we have a backward relationship. In this case, the
                # converse relationship name is preferred if it exists.
                my $converse = $relationship->{converse} || $relationshipName;
                push @retVal, $converse;
            }
        }
    }
    # Return the result.
    return @retVal;
}

=head3 GetConnectingRelationshipData

    my ($froms, $tos) = $erdb->GetConnectingRelationshipData($entityName);

Return the relationship data for the specified entity. The return will be
a two-element list, each element of the list a reference to a hash that
maps relationship names to structures. The first hash will be
relationships originating from the entity, and the second element a
reference to a hash of relationships pointing to the entity.

=over 4

=item entityName

Name of the entity of interest.

=item RETURN

Returns a two-element list, each list being a map of relationship names
to relationship metadata structures. The first element lists relationships
originating from the entity, and the second element lists relationships that
point to the entity.

=back

=cut

sub GetConnectingRelationshipData {
    # Get the parameters.
    my ($self, $entityName) = @_;
    # Create a hash that holds the return values.
    my %retVal = (from => {}, to => {});
    # Get the relationship table in the metadata.
    my $relationships = $self->{_metaData}->{Relationships};
    # Loop through it twice, once for each direction.
    for my $direction (qw(from to)) {
        # Get the return hash for this direction.
        my $hash = $retVal{$direction};
        # Loop through the relationships, looking for our entity in the
        # current direction.
        for my $rel (keys %$relationships) {
            my $relData = $relationships->{$rel};
            if ($relData->{$direction} eq $entityName) {
                # Here we've found our entity, so we put it in the
                # return hash.
                $hash->{$rel} = $relData;
            }
        }
    }
    # Return the results.
    return ($retVal{from}, $retVal{to});
}

=head3 GetDataTypes

    my $types = ERDBtk::GetDataTypes();

Return a table of ERDBtk data types. The table returned is a hash of
L</ERDBtk::Type> objects keyed by type name.

=cut

sub GetDataTypes {
    # Insure we have a type table.
    if (! defined $TypeTable) {
        # Get a list of the names of the standard type classes.
        my @types = @StandardTypes;
        # Add in the custom types, if any.
        if (defined $ERDBtkExtras::customERDBtktypes) {
            push @types, @$ERDBtkExtras::customERDBtktypes;
        }
        # Initialize the table.
        $TypeTable = {};
        # Loop through all of the types, creating the type objects.
        for my $type (@types) {
            # Create the type object.
            my $typeObject;
            eval {
                require "ERDBtk/Type/$type.pm";
                $typeObject = eval("ERDBtk::Type::$type->new()");
            };
            # Ensure we didn't have an error.
            if ($@) {
                Confess("Error building ERDBtk type table: $@");
            } else {
                # Add the type to the type table.
                $TypeTable->{$typeObject->name()} = $typeObject;
            }
        }
    }
    # Return the type table.
    return $TypeTable;
}


=head3 ShowDataTypes

    my $markup = ERDBtk::ShowDataTypes($wiki, $erdb);

Display a table of all the valid data types for this installation.

=over 4

=item wiki

An object used to render the table, similar to L</WikiTools>.

=item erdb (optional)

If specified, an ERDBtk object for a specific database. Only types used by
the database will be put in the table. If omitted, all types are returned.


=back

=cut

sub ShowDataTypes {
    my ($wiki, $erdb) = @_;
    # Compute the hash of types to display.
    my $typeHash = ();
    if (! defined $erdb) {
        # No ERDBtk object, so we list all the types.
        $typeHash = GetDataTypes();
    } else {
        # Here we must extract the types used in the ERDBtk object.
        for my $relationName ($erdb->GetTableNames()) {
            my $relationData = $erdb->FindRelation($relationName);
            for my $fieldData (@{$relationData->{Fields}}) {
                my $type = $fieldData->{type};
                my $typeData = $TypeTable->{$type};
                if (! defined $typeData) {
                    Confess("Invalid data type \"$type\" in relation $relationName.");
                } else {
                    $typeHash->{$type} = $typeData;
                }
            }
        }
    }
    # We'll build table rows in here. We start with the header.
    my @rows = [qw(Type Indexable Sort Pos Format Description)];
    # Loop through the types, generating rows.
    for my $type (sort keys %$typeHash) {
        # Get the type object.
        my $typeData = $typeHash->{$type};
        # Compute the indexing column.
        my $flag = $typeData->indexMod();
        if (! defined $flag) {
            $flag = "no";
        } elsif ($flag eq "") {
            $flag = "yes";
        } else {
            $flag = "prefix";
        }
        # Compute the sort type.
        my $sortType = $typeData->sortType();
        if ($sortType eq 'g' || $sortType eq 'n') {
            $sortType = "numeric";
        } else {
            $sortType = "alphabetic";
        }
        # Get the position (pretty-sort value).
        my $pos = $typeData->prettySortValue();
        # Finally, the format.
        my $format = $typeData->objectType() || "scalar";
        # Build the data row.
        my $row = [$type, $flag, $sortType, $pos, $format, $typeData->documentation()];
        # Put it into the table.
        push @rows, $row;
    }
    # Form up the table.
    my $retVal = $wiki->Table(@rows);
    # Return the result.
    return $retVal;
}

=head3 IsEntity

    my $flag = $erdb->IsEntity($entityName);

Return TRUE if the parameter is an entity name, else FALSE.

=over 4

=item entityName

Object name to be tested.

=item RETURN

Returns TRUE if the specified string is an entity name, else FALSE.

=back

=cut

sub IsEntity {
    # Get the parameters.
    my ($self, $entityName) = @_;
    # Test to see if it's an entity.
    return exists $self->{_metaData}->{Entities}->{$entityName};
}

=head3 GetSecondaryFields

    my %fieldTuples = $erdb->GetSecondaryFields($entityName);

This method will return a list of the name and type of each of the secondary
fields for a specified entity. Secondary fields are stored in two-column tables
separate from the primary entity table. This enables the field to have no value
or to have multiple values.

=over 4

=item entityName

Name of the entity whose secondary fields are desired.

=item RETURN

Returns a hash mapping the field names to their field types.

=back

=cut

sub GetSecondaryFields {
    # Get the parameters.
    my ($self, $entityName) = @_;
    # Declare the return variable.
    my %retVal = ();
    # Look for the entity.
    my $table = $self->GetFieldTable($entityName);
    # Loop through the fields, pulling out the secondaries.
    for my $field (sort keys %{$table}) {
        if ($table->{$field}->{relation} ne $entityName) {
            # Here we have a secondary field.
            $retVal{$field} = $table->{$field}->{type};
        }
    }
    # Return the result.
    return %retVal;
}

=head3 GetFieldRelationName

    my $name = $erdb->GetFieldRelationName($objectName, $fieldName);

Return the name of the relation containing a specified field.

=over 4

=item objectName

Name of the entity or relationship containing the field.

=item fieldName

Name of the relevant field in that entity or relationship.

=item RETURN

Returns the name of the database relation containing the field, or C<undef> if
the field does not exist.

=back

=cut

sub GetFieldRelationName {
    # Get the parameters.
    my ($self, $objectName, $fieldName) = @_;
    # Declare the return variable.
    my $retVal;
    # Get the object field table.
    my $table = $self->GetFieldTable($objectName);
    # Only proceed if the field exists.
    if (exists $table->{$fieldName}) {
        # Determine the name of the relation that contains this field.
        $retVal = $table->{$fieldName}->{relation};
    }
    # Return the result.
    return $retVal;
}

=head3 DumpMetaData

    $erdb->DumpMetaData();

Return a dump of the metadata structure.

=cut

sub DumpMetaData {
    # Get the parameters.
    my ($self) = @_;
    # Dump the meta-data.
    return Data::Dumper::Dumper($self->{_metaData});
}


=head3 CheckObjectNames

    my @errors = $erdb->CheckObjectNames($objectNameString);

Check an object name string for errors. The return value will be a list
of error messages. If no error is found, an empty list will be returned.
This process does not guarantee a correct object name list, but it
catches the most obvious errors without the need for invoking a
full-blown L</Get> method.

=over 4

=item objectNameString

An object name string, consisting of a space-delimited list of entity and
relationship names.

=item RETURN

Returns an empty list if successful, and a list of error messages if the
list is invalid.

=back

=cut

sub CheckObjectNames {
    # Get the parameters.
    my ($self, $objectNameString) = @_;
    # Declare the return variable.
    my @retVal;
    # Separate the string into pieces.
    my @objectNames = split m/\s+/, $objectNameString;
    # Start in a blank state.
    my $currentObject;
    # Get the alias and crossing tables.
    my $aliasTable = $self->{_metaData}{AliasTable};
    my $crossTable = $self->{_metaData}{CrossingTable};
    my $jumpTable = $self->{_metaData}{JumpTable};
    # Loop through the object names.
    for my $objectName (@objectNames) {
        # If we have an AND, clear the current object.
        if ($objectName eq 'AND') {
            # Insure we don't have an AND at the beginning or after another AND.
            if (! defined $currentObject) {
                push @retVal, "An AND was found in the wrong place.";
            }
            # Clear the context.
            undef $currentObject;
        } else {
            # Here the user has specified an object name. Get
            # the root name.
            unless ($objectName =~ /^(.+?)(\d*)$/) {
                # Here the name has bad characters in it. Note that an error puts
                # us into a blank state.
                push @retVal, "Invalid characters found in \"$objectName\".";
                undef $currentObject;
            } else {
                # Get the real name from the alias table.
                my $newObject = $1;
                my $name = $aliasTable->{$newObject};
                if (! defined $name) {
                    push @retVal, "Could not find an entity or relationship named \"$objectName\".";
                    undef $currentObject;
                } else {
                    # Okay, we've got the real entity or relationship name. Does it belong here?
                    # That's only an issue if there is a previous value in $currentObject.
                    if (defined $currentObject && ! defined $crossTable->{$currentObject}{$newObject} &&
                        ! defined $jumpTable->{$currentObject}{$newObject}) {
                        push @retVal, "There is no connection between $currentObject and $newObject."
                    }
                    # Save this object as the new current object.
                    $currentObject = $newObject;
                }
            }
        }
    }
    # Return the result.
    return @retVal;
}

=head3 JumpCheck

    my $pathTable = $erdb->JumpCheck($object1, $object2);

Determine if there is a path between the first and second object. If one exists, the name
of the intermediate object will be returned.

=over 4

=item object1

The source object for the jump.

=item object2

The target object for the jump.

=item RETURN

Returns the name of the object that facilitates the path, or C<undef> if there is no
direct jump between the two objects.

=back

=cut

sub JumpCheck {
    # Get the parameters.
    my ($self, $object1, $object2) = @_;
    # Get the jump table.
    my $jumpTable = $self->{_metaData}{JumpTable};
    # Return the jump determination.
    return $jumpTable->{$object1}{$object2};
}

=head3 GetTitle

    my $text = $erdb->GetTitle();

Return the title for this database.

=cut

sub GetTitle {
    # Get the parameters.
    my ($self) = @_;
    # Declare the return variable.
    my $retVal = $self->{_metaData}->{Title};
    if (! $retVal) {
        # Here no title was supplied, so we make one up.
        $retVal = "Unknown Database";
    } else {
        # Extract the content of the title element. This is the real title.
        $retVal = $retVal->{content};
    }
    # Return the result.
    return $retVal;
}

=head3 GetDiagramOptions

    my $hash = $erdb->GetDiagramOptions();

Return the diagram options structure for this database. The diagram
options are used by the ERDBtk documentation widget to configure the
database diagram. If the options are not present, an undefined value will
be returned.

=cut

sub GetDiagramOptions {
    # Get the parameters.
    my ($self) = @_;
    # Extract the options element.
    my $retVal = $self->{_metaData}->{Diagram};
    # Return the result.
    return $retVal;
}

=head3 GetMetaFileName

    my $fileName = $erdb->GetMetaFileName();

Return the name of the database definition file for this database.

=cut

sub GetMetaFileName {
    # Get the parameters.
    my ($self) = @_;
    # Return the result.
    return $self->{_metaFileName};
}

=head3 IsEmbedded

    my $flag = $erdb->IsEmbedded($objectName);

Returns TRUE if the specified object is an embedded relationship, else
FALSE.

=over 4

=item objectName

Name of the object (entity or relationship) whose embedded status is desired.

=item RETURN

Returns TRUE if the object is an embedded relationship, else FALSE.

=back

=cut

sub IsEmbedded {
    # Get the parameters.
    my ($self, $objectName) = @_;
    # Declare the return variable.
    my $retVal;
    # Is this a relationship?
    my $relData = $self->FindRelationship($objectName);
    if ($relData) {
        # Yes, return the embed flag.
        $retVal = $relData->{embedded};
    }
    # Return the result.
    return $retVal;
}


=head2 Database Administration and Loading Methods

=head3 db

    my $erdb = $erdb->db;

Return this object. This method allows the ERDBtk object itself to be passed around as
a loader to the ID helpers.

=cut

sub db {
    return $_->[0];
}


=head3 LoadTable

    my $results = $erdb->LoadTable($fileName, $relationName, %options);

Load data from a tab-delimited file into a specified table, optionally
re-creating the table first.

=over 4

=item fileName

Name of the file from which the table data should be loaded.

=item relationName

Name of the relation to be loaded. This is the same as the table name.

=item options

A hash of load options.

=item RETURN

Returns a statistical object containing a list of the error messages.

=back

The permissible options are as follows.

=over 4

=item truncate

If TRUE, then the table will be erased before loading.

=item mode

Mode in which the load should operate, either C<low_priority> or C<concurrent>.
This option is only applicable to a MySQL database.

=item partial

If TRUE, then it is assumed that this is a partial load, and the table will not
be analyzed and compacted at the end.

=item failOnError

If TRUE, then when an error occurs, the process will be killed; otherwise, the
process will stay alive, but a message will be put into the statistics object.

=item dup

If C<ignore>, duplicate rows will be ignored. If C<replace>, duplicate rows will
replace previous instances. If omitted, duplicate rows will cause an error.

=back

=cut

sub LoadTable {
    # Get the parameters.
    my ($self, $fileName, $relationName, %options) = @_;
    # Record any error message in here. If it's defined when we're done
    # and failOnError is set, we confess it.
    my $errorMessage;
    # Create the statistical return object.
    my $retVal = _GetLoadStats();
    # Trace the fact of the load.
    # Get the database handle.
    my $dbh = $self->{_dbh};
    # Get the input file size.
    my $fileSize = -s $fileName;
    # Get the relation data.
    my $relation = $self->FindRelation($relationName);
    # Check the truncation flag.
    if ($options{truncate}) {
        # Compute the row count estimate. We take the size of the load file,
        # divide it by the estimated row size, and then multiply by 8 to
        # leave extra room. We postulate a minimum row count of 10000 to
        # prevent problems with incoming empty load files.
        my $rowSize = $self->EstimateRowSize($relationName);
        my $estimate = $fileSize * 8 / $rowSize;
        if ($estimate < 10000) {
            $estimate = 10000;
        }
        # Re-create the table without its index.
        $self->CreateTable($relationName, unindexed => 1, estimate => $estimate);
        # If this is a pre-index DBMS, create the index here.
        if ($dbh->{_preIndex}) {
            eval {
                $self->CreateIndexes($relationName);
            };
            if ($@) {
                $retVal->AddMessage($@);
                $errorMessage = $@;
            }
        }
    }
    # Load the table.
    my $rv;
    eval {
        my $q = $self->q;
        $rv = $dbh->load_table(file => $fileName, tbl => "$q$relationName$q",
                               style => $options{mode}, 'local' => 'LOCAL',
                               dup => $options{dup} );
    };
    if (!defined $rv) {
        $retVal->AddMessage($@) if ($@);
        $errorMessage = "Table load failed for $relationName using $fileName.";
        $retVal->AddMessage("$errorMessage: " . $dbh->error_message);
    } else {
        # Here we successfully loaded the table.
        my $size = -s $fileName;
        $retVal->Add("bytes-loaded", $size);
        $retVal->Add("tables-loaded" => 1);
        # If we're rebuilding, we need to create the table indexes.
        if ($options{truncate}) {
            # Indexes are created here for PostGres. For PostGres, indexes are
            # best built at the end. For MySQL, the reverse is true.
            if (! $dbh->{_preIndex}) {
                eval {
                    $self->CreateIndexes($relationName);
                };
                if ($@) {
                    $errorMessage = $@;
                    $retVal->AddMessage($errorMessage);
                }
            }
        }
    }
    if ($errorMessage && $options{failOnError}) {
        # Here the load failed and we want to error out.
        Confess($errorMessage);
    }
    # Return the statistics.
    return $retVal;
}


=head3 Analyze

    $erdb->Analyze($tableName);

Analyze and compact a table in the database. This is useful after a load
to improve the performance of the indexes.

=over 4

=item tableName

Name of the table to be analyzed and compacted.

=back

=cut

sub Analyze {
    # Get the parameters.
    my ($self, $tableName) = @_;
    # Analyze the table.
    my $dbh = $self->{_dbh};
    my $q = $self->q;
    $dbh->vacuum_it("$q$tableName$q");
}

=head3 TruncateTable

    $erdb->TruncateTable($table);

Delete all rows from a table quickly. This uses the built-in SQL
C<TRUNCATE> statement, which effectively drops and re-creates a table
with all its settings intact.

=over 4

=item table

Name of the table to be cleared.

=back

=cut

sub TruncateTable {
    # Get the parameters.
    my ($self, $table) = @_;
    # Get the database handle.
    my $dbh = $self->{_dbh};
    my $q = $self->q;
    # Execute a truncation comment.
    $dbh->truncate_table("$q$table$q");
}

=head3 DropRelation

    $erdb->DropRelation($relationName);

Physically drop a relation from the database.

=over 4

=item relationName

Name of the relation to drop. If it does not exist, this method will have
no effect.

=back

=cut

sub DropRelation {
    # Get the parameters.
    my ($self, $relationName) = @_;
    # Get the database handle.
    my $dbh = $self->{_dbh};
    my $q = $self->q;
    # Drop the relation. The method used here has no effect if the relation
    # does not exist.
    $dbh->drop_table(tbl => "$q$relationName$q");
}

=head3 DumpRelations

    $erdb->DumpRelations($outputDirectory);

Write the contents of all the relations to tab-delimited files in the specified directory.
Each file will have the same name as the relation dumped, with an extension of DTX.

=over 4

=item outputDirectory

Name of the directory into which the relation files should be dumped.

=back

=cut

sub DumpRelations {
    # Get the parameters.
    my ($self, $outputDirectory) = @_;
    # Now we need to run through all the relations. First, we loop through the entities.
    my $metaData = $self->{_metaData};
    my $entities = $metaData->{Entities};
    for my $entityName (keys %{$entities}) {
        my $entityStructure = $entities->{$entityName};
        # Get the entity's relations.
        my $relationList = $entityStructure->{Relations};
        # Loop through the relations, dumping them.
        for my $relationName (keys %{$relationList}) {
            $self->_DumpRelation($outputDirectory, $relationName);
        }
    }
    # Next, we loop through the relationships.
    my $relationships = $metaData->{Relationships};
    for my $relationshipName (keys %{$relationships}) {
        # Are we embedded?
        if (! $self->IsEmbedded($relationshipName)) {
            # No. Dump this relationship's relation.
            $self->_DumpRelation($outputDirectory, $relationshipName);
        }
    }
}

=head3 DumpTable

    my $count = $erdb->DumpTable($tableName, $directory);

Dump the specified table to the named directory. This will create a load
file having the same name as the relation with an extension of DTX. This
file can then be used to reload the table at a later date. If the table
does not exist, no action will be taken.

=over 4

=item tableName

Name of the table to dump.

=item directory

Name of the directory in which the dump file should be placed.

=item RETURN

Returns the number of records written.

=back

=cut

sub DumpTable {
    # Get the parameters.
    my ($self, $tableName, $directory) = @_;
    # Declare the return variable.
    my $retVal;
    # Insure the table name is valid.
    if (exists $self->{_metaData}->{RelationTable}->{$tableName}) {
        # Call the internal dumper.
        $retVal = $self->_DumpRelation($directory, $tableName);
    }
    # Return the result.
    return $retVal;
}


=head3 TypeDefault

    my $value = ERDBtk::TypeDefault($type);

Return the default value for fields of the specified type.

=over 4

=item type

Relevant type name.

=item RETURN

Returns a default value suitable for fields of the specified type.

=back

=cut

sub TypeDefault {
    # Get the parameters.
    my ($type) = @_;
    # Validate the type.
    if (! exists $TypeTable->{$type}) {
        Confess("TypeDefault called for invalid type \"$type\".")
    }
    # Return the result.
    return $TypeTable->{$type}->default();
}

=head3 LoadTables

    my $stats = $erdb->LoadTables($directoryName, $rebuild);

This method will load the database tables from a directory. The tables must
already have been created in the database. (This can be done by calling
L</CreateTables>.) The caller passes in a directory name; all of the relations
to be loaded must have a file in the directory with the same name as the
relation with a suffix of C<.dtx>. Each file must be a tab-delimited table of
encoded field values. Each line of the file will be loaded as a row of the
target relation table.

=over 4

=item directoryName

Name of the directory containing the relation files to be loaded.

=item rebuild

TRUE if the tables should be dropped and rebuilt, else FALSE.

=item RETURN

Returns a L</Stats> object describing the number of records read and a list of
the error messages.

=back

=cut

sub LoadTables {
    # Get the parameters.
    my ($self, $directoryName, $rebuild) = @_;
    # Start the timer.
    my $startTime = gettimeofday;
    # Clean any trailing slash from the directory name.
    $directoryName =~ s!/\\$!!;
    # Declare the return variable.
    my $retVal = Stats->new();
    # Get the relation names.
    my @relNames = $self->GetTableNames();
    for my $relationName (@relNames) {
        # Try to load this relation.
        my $result = $self->_LoadRelation($directoryName, $relationName,
                                          $rebuild);
        # Accumulate the statistics.
        $retVal->Accumulate($result);
    }
    # Add the duration of the load to the statistical object.
    $retVal->Add('duration', gettimeofday - $startTime);
    # Return the accumulated statistics.
    return $retVal;
}

=head3 CreateTables

    $erdb->CreateTables();

This method creates the tables for the database from the metadata structure
loaded by the constructor. It is expected this function will only be used on
rare occasions, when the user needs to start with an empty database. Otherwise,
the L</LoadTables> method can be used by itself with the truncate flag turned
on.

=cut

sub CreateTables {
    # Get the parameters.
    my ($self) = @_;
    # Get the relation names.
    my @relNames = $self->GetTableNames();
    # Loop through the relations.
    for my $relationName (@relNames) {
        # Create a table for this relation.
        $self->CreateTable($relationName);
    }
}

=head3 CreateTable

    $erdb->CreateTable($tableName, %options);

Create the table for a relation and optionally create its indexes.

=over 4

=item relationName

Name of the relation (which will also be the table name).

=item options

A hash of options, including zero or more of the following.

=over 4

=item unindexed

If TRUE, no indexes will be created for the relation. If this option is
specified, L</CreateIndex> must be called later to bring the indexes
into existence.

=item nodrop

If TRUE, the table will not be dropped before creation. This will cause an
error if the table already exists.

=back

=back

=cut

sub CreateTable {
    # Get the parameters.
    my ($self, $relationName, %options) = @_;
    # Get the database handle.
    my $dbh = $self->{_dbh};
    # Get the quote character.
    my $q = $self->q;
    # Determine whether or not the relation is primary.
    my $rootFlag = $self->_IsPrimary($relationName);
    # Create a list of the field data.
    my $fieldThing = $self->ComputeFieldString($relationName);
    # Insure the table is not already there.
    if (! $options{nodrop}) {
        $dbh->drop_table(tbl => "$q$relationName$q");
    }
    my ($engine, $estimation);
    if ($rootFlag) {
        my $entity = $self->FindEntity($relationName);
        # Check for an engine.
        if ($entity->{engine}) {
            $engine = $entity->{engine};
        }
        # Create an estimate of the table size.
        if ($entity->{estimate}) {
            $estimation = [$self->EstimateRowSize($relationName), $entity->{estimate}];
        }
    }
    # Create the table.
    $dbh->create_table(tbl => $q . $relationName . $q, flds => $fieldThing,
                       estimates => $estimation, engine => $engine);
    # If we want to build the indexes, we do it here. Note that the full-text
    # search index will not be built until the table has been loaded.
    if (! $options{unindexed}) {
        $self->CreateIndexes($relationName);
    }
}

=head3 ComputeFieldString

    my $fieldString = $erdb->ComputeFieldString($relationName);

Return the comma-delimited field definition string for a relation. This can be plugged directly into an SQL
C<CREATE> statement.

=over 4

=item relationName

Name of the relation whose field definition string is desired.

=item RETURN

Returns a string listing SQL field definitions, in the proper order, separated by commas.

=back

=cut

sub ComputeFieldString {
    # Get the parameters.
    my ($self, $relationName) = @_;
    # Get the relation data.
    my $relationData = $self->FindRelation($relationName);
    # Create a list of the field data.
    my @fieldList;
    for my $fieldData (@{$relationData->{Fields}}) {
        # Assemble the field name and type.
        my $fieldString = $self->_FieldString($fieldData);
        # Push the result into the field list.
        push @fieldList, $fieldString;
    }
    # Convert the field list into a comma-delimited string.
    my $retVal = join(', ', @fieldList);
    return $retVal;
}

=head3 VerifyFields

    $erdb->VerifyFields($relName, \@fieldList);

Run through the list of proposed field values, insuring that all of them are
valid.

=over 4

=item relName

Name of the relation for which the specified fields are destined.

=item fieldList

Reference to a list, in order, of the fields to be put into the relation.

=back

=cut

sub VerifyFields {
    # Get the parameters.
    my ($self, $relName, $fieldList) = @_;
    # Initialize the return value.
    my $retVal = 0;
    # Get the relation definition.
    my $relData = $self->FindRelation($relName);
    # Get the list of field descriptors.
    my $fieldThings = $relData->{Fields};
    my $fieldCount = scalar @{$fieldThings};
    # Loop through the two lists.
    for (my $i = 0; $i < $fieldCount; $i++) {
        # Get the descriptor and type of the current field.
        my $fieldThing = $fieldThings->[$i];
        my $fieldType = $TypeTable->{$fieldThing->{type}};
        Confess("Undefined field type $fieldThing->{type} in position $i ($fieldThing->{name}) of $relName.") if (! defined $fieldType);
        # Validate it.
        my $message = $fieldType->validate($fieldList->[$i]);
        if ($message) {
            # It's invalid. Generate an error.
            Confess("Error in field $i ($fieldThing->{name}) of $relName: $message");
        }
    }
    # Return a 0 value, for backward compatibility.
    return 0;
}

=head3 DigestFields

    $erdb->DigestFields($relName, $fieldList);

Prepare the fields of a relation for output to a load file.

=over 4

=item relName

Name of the relation to which the fields belong.

=item fieldList

List of field contents to be loaded into the relation.

=back

=cut
#: Return Type ;
sub DigestFields {
    # Get the parameters.
    my ($self, $relName, $fieldList) = @_;
    # Get the relation definition.
    my $relData = $self->FindRelation($relName);
    # Get the list of field descriptors.
    my $fieldTypes = $relData->{Fields};
    my $fieldCount = scalar @{$fieldTypes};
    # Loop through the two lists.
    for (my $i = 0; $i < $fieldCount; $i++) {
        # Get the type of the current field.
        my $fieldType = $fieldTypes->[$i]->{type};
        # Encode the field value in place.
        $fieldList->[$i] = $TypeTable->{$fieldType}->encode($fieldList->[$i], 1);
    }
}

=head3 EncodeField

    my $coding = $erdb->EncodeField($fieldName, $value);

Convert the specified value to the proper format for storing in the
specified database field. The field name should be specified in the
standard I<object(field)> format, e.g. C<Feature(id)> for the C<id> field
of the C<Feature> table.

=over 4

=item fieldName

Name of the field, specified in as an object name with the field name
in parentheses.

=item value

Value to encode for placement in the field.

=item RETURN

Coded value ready to put in the database. In most cases, this will be
identical to the original input.

=back

=cut

sub EncodeField {
    # Get the parameters.
    my ($self, $fieldName, $value) = @_;
    # Find the field type.
    my $fieldSpec = $self->_FindField($fieldName);
    my $retVal = encode($fieldSpec->{type}, $value);
    # Return the result.
    return $retVal;
}

=head3 encode

    my $coding = ERDBtk::encode($type, $value);

Encode a value of the specified type for storage in the database or for
use as a query parameter. Encoding is automatic for all ERDBtk methods except
when loading a table from a user-supplied load file or when processing
the parameters for a query filter string. This method can be used in
those situations to remedy the lack.

=over 4

=item type

Name of the incoming value's data type.

=item value

Value to encode into a string.

=item RETURN

Returns the encoded value.

=back

=cut

sub encode {
    # Get the parameters.
    my ($type, $value) = @_;
    # Get the type definition.
    my $typeData = $TypeTable->{$type};
    # Complain if it doesn't exist.
    Confess("Invalid data type \"$type\" specified in encoding.") if ! defined $typeData;
    # Encode the value.
    my $retVal = $typeData->encode($value);
    # Return the result.
    return $retVal;
}

=head3 DecodeField

    my $value = $erdb->DecodeField($fieldName, $coding);

Convert the stored coding of the specified field to the proper format for
use by the client program. This is essentially the inverse of
L</EncodeField>.

=over 4

=item fieldName

Name of the field, specified as an object name with the field name
in parentheses.

=item coding

Coded data from the database.

=item RETURN

Returns the original form of the coded data.

=back

=cut

sub DecodeField {
    # Get the parameters.
    my ($self, $fieldName, $coding) = @_;
    # Declare the return variable.
    my $retVal = $coding;
    # Get the field type.
    my $fieldSpec = $self->_FindField($fieldName);
    my $type = $fieldSpec->{type};
    # Process according to the type.
    $retVal = $TypeTable->{$type}->decode($coding);
    # Return the result.
    return $retVal;
}


=head3 DigestKey

    my $digested = ERDBtk::DigestKey($longString);

Return the digested value of a string. The digested value is a fixed
length (22 characters) MD5 checksum. It can be used as a more convenient
version of a symbolic key.

=over 4

=item longString

String to digest.

=item RETURN

Digested value of the string.

=back

=cut

sub DigestKey {
    # Allow object-based calls for backward compatability.
    shift if UNIVERSAL::isa($_[0], __PACKAGE__);
    # Get the parameters.
    my ($keyValue) = @_;
    # Compute the digest.
    my $retVal = md5_base64($keyValue);
    # Return the result.
    return $retVal;
}

=head3 CreateIndexes

    $erdb->CreateIndexes($relationName);

Create the indexes for a relation. If a table is being loaded from a large
source file (as is the case in L</LoadTable>), it is sometimes best to create
the indexes after the load. If that is the case, then L</CreateTable> should be
called with the index flag set to FALSE, and this method used after the load to
create the indexes for the table.

=over 4

=item relationName

Name of the relation whose indexes are to be created.

=back

=cut

sub CreateIndexes {
    # Get the parameters.
    my ($self, $relationName) = @_;
    # Get the relation's descriptor.
    my $relationData = $self->FindRelation($relationName);
    # Get the database handle.
    my $dbh = $self->{_dbh};
    # Now we need to create this relation's indexes. We do this by looping
    # through its index table.
    my $indexHash = $relationData->{Indexes};
    for my $indexName (keys %{$indexHash}) {
        $self->CreateIndex($relationName, $indexName);
    }
}

=head3 CreateIndex

    $erdb->CreateIndex($relationName, $indexName);

Create the index on the specified relation with the specified name.

=over 4

=item relationName

Name of the relation on which the index is to be created.

=item indexName

Name of the index in the relation's index set.

=back

=cut

sub CreateIndex {
    # Get the parameters.
    my ($self, $relationName, $indexName) = @_;
    # Get the index descriptor.
    my $relationData = $self->FindRelation($relationName);
    my $indexData = $relationData->{Indexes}{$indexName};
    # Get the DBtk handle.
    my $dbh = $self->{_dbh};
    # Get the quote character.
    my $q = $self->q;
    # Get the index's field list.
    my @rawFields = @{$indexData->{IndexFields}};
    # Get a hash of the relation's field types.
    my %types = map { $_->{name} => $_->{type} } @{$relationData->{Fields}};
    # We need to check for partial-indexed fields so we can append a length limitation
    # for them. To do that, we need the relation's field list.
    my $relFields = $relationData->{Fields};
    for (my $i = 0; $i <= $#rawFields; $i++) {
        # Split the ordering suffix from the field name.
        my ($field, $suffix) = split(/\s+/, $rawFields[$i]);
        $suffix = "" if ! defined $suffix;
        # Get the field type.
        my $type = $types{$field};
        # Ask if it requires using prefix notation for the index.
        my $mod = $TypeTable->{$type}->indexMod();
        if (! defined($mod)) {
            Confess("Non-indexable type $type specified for index field in $relationName.");
        } elsif ($mod) {
            # Here we have an indexed field that requires a modification in order
            # to work. This means we need to insert it between the
            # field name and the ordering suffix. Note we make sure the
            # suffix is defined.
            $rawFields[$i] =  join(" ", $dbh->index_mod($q . $field . $q, $mod), $suffix);
        } else {
            # Here we have a normal field, so we quote it.
            $rawFields[$i] = join(" ", $q . $field . $q, $suffix);
        }
    }
    my @fieldList = _FixNames(@rawFields);
    my $flds = join(', ', @fieldList);
    # Get the index's uniqueness flag.
    my $unique = ($indexData->{primary} ? 'primary' : ($indexData->{unique} ? 'unique' : undef));
    # Compute the name to use. The primary index is called PRIMARY, but we need to give it a different
    # name in the create statement.
    my $actualName = ($indexData->{primary} ? "idxP$relationName" : $indexName);
    # Create the index.
    my $rv = $dbh->create_index(idx => "$q$actualName$q", tbl => "$q$relationName$q",
                                flds => $flds, kind => $unique);
    if (! $rv) {
        Confess("Error creating index $indexName for $relationName using ($flds): " .
                $dbh->error_message());
    }
}


=head3 SetTestEnvironment

    $erdb->SetTestEnvironment();

Denote that this is a test environment. Certain performance-enhancing
features may be disabled in a test environment.

=cut

sub SetTestEnvironment {
    # Get the parameters.
    my ($self) = @_;
    # Tell the database we're in test mode.
    $self->{_dbh}->test_mode();
}

=head3 dbName

    my $dbName = $erdb->dbName();

Return the physical name of the database currently attached to this object.

=cut

sub dbName {
    # Get the parameters.
    my ($self) = @_;
    # We'll return the database name in here.
    my $retVal;
    # Get the connection string.
    my $connect = $self->{_dbh}->{_connect};
    # Extract the database name.
    if ($connect =~ /dbname\=([^;])/) {
        $retVal = $1;
    }
    # Return the result.
    return $retVal;
}


=head2 Database Update Methods

=head3 BeginTran

    $erdb->BeginTran();

Start a database transaction.

=cut

sub BeginTran {
    my ($self) = @_;
    $self->{_dbh}->begin_tran();

}

=head3 CommitTran

    $erdb->CommitTran();

Commit an active database transaction.

=cut

sub CommitTran {
    my ($self) = @_;
    $self->{_dbh}->commit_tran();
}

=head3 RollbackTran

    $erdb->RollbackTran();

Roll back an active database transaction.

=cut

sub RollbackTran {
    my ($self) = @_;
    $self->{_dbh}->roll_tran();
}

=head3 UpdateField

    my $count = $erdb->UpdateField($fieldName, $oldValue, $newValue, $filter, $parms);

Update all occurrences of a specific field value to a new value. The number of
rows changed will be returned.

=over 4

=item fieldName

Name of the field in L</Standard Field Name Format>.

=item oldValue

Value to be modified. All occurrences of this value in the named field will be
replaced by the new value.

=item newValue

New value to be substituted for the old value when it's found.

=item filter

A standard ERDBtk filter clause. See L</Filter Clause>. The filter will be applied before
any substitutions take place. Note that the filter clause in this case must only
specify fields in the table containing fields.

=item parms

Reference to a list of parameter values in the filter. See L</Parameter List>.

=item RETURN

Returns the number of rows modified.

=back

=cut

sub UpdateField {
    # Get the parameters.
    my ($self, $fieldName, $oldValue, $newValue, $filter, $parms) = @_;
    # Get the object and field names from the field name parameter.
    my ($objectName, $realFieldName) = ERDBtk::ParseFieldName($fieldName);
    # Add the old value to the filter. Note we allow the possibility that no
    # filter was specified.
    my $realFilter = "$fieldName = ?";
    if ($filter) {
        $realFilter .= " AND ($filter)";
    }
    # Format the query filter.
    my $sqlHelper = ERDBtk::Helpers::SQLBuilder->new($self, $objectName);
    my $suffix = $sqlHelper->SetFilterClause($realFilter);
    # Create the update statement. Note we need to get rid of the FROM clause
    # and the field list is a single name.
    $suffix =~ s/^FROM.+WHERE\s+//;
    my $fieldList = $sqlHelper->ComputeFieldList($fieldName);
    # Get the database handle.
    my $dbh = $self->{_dbh};
    my $q = $self->q;
    # Create the update statement.
    my $command = "UPDATE $q$objectName$q SET $fieldList = ? WHERE $suffix";
    # Add the old and new values to the parameter list. Note we allow the
    # possibility that there are no user-supplied parameters.
    my @params = ($newValue, $oldValue);
    if (defined $parms) {
        push @params, @{$parms};
    }
    # Execute the update.
    my $retVal = $dbh->SQL($command, 0, @params);
    # Make the funky zero a real zero.
    if ($retVal == 0) {
        $retVal = 0;
    }
    # Return the result.
    return $retVal;
}

=head3 InsertValue

    $erdb->InsertValue($entityID, $fieldName, $value);

This method will insert a new value into the database. The value must be one
associated with a secondary relation, since primary values cannot be inserted:
they occur exactly once. Secondary values, on the other hand, can be missing
or multiply-occurring.

=over 4

=item entityID

ID of the object that is to receive the new value.

=item fieldName

Field name for the new value in L</Standard Field Name Format>. This specifies
the entity name and the field name in a single string.

=item value

New value to be put in the field.

=back

=cut

sub InsertValue {
    # Get the parameters.
    my ($self, $entityID, $fieldName, $value) = @_;
    # Get the quote character.
    my $q = $self->q;
    # Parse the entity name and the real field name.
    my ($entityName, $fieldTitle) = ERDBtk::ParseFieldName($fieldName);
    if (! defined $entityName) {
        Confess("Invalid field name specification \"$fieldName\" in InsertValue call.");
    } else {
        # Insure we are in an entity.
        if (!$self->IsEntity($entityName)) {
            Confess("$entityName is not a valid entity.");
        } else {
            my $entityData = $self->{_metaData}->{Entities}->{$entityName};
            # Find the relation containing this field.
            my $fieldHash = $entityData->{Fields};
            if (! exists $fieldHash->{$fieldTitle}) {
                Confess("$fieldTitle not found in $entityName.");
            } else {
                my $relation = $fieldHash->{$fieldTitle}->{relation};
                if ($relation eq $entityName) {
                    Confess("Cannot do InsertValue on primary field $fieldTitle of $entityName.");
                } else {
                    # Now we can create an INSERT statement.
                    my $dbh = $self->{_dbh};
                    my $fixedName = _FixName($fieldTitle);
                    my $statement = "INSERT INTO $q$relation$q (id, $q$fixedName$q) VALUES(?, ?)";
                    # Execute the command.
                    my $codedValue = $self->EncodeField($fieldName, $value);
                    $dbh->SQL($statement, 0, $entityID, $codedValue);
                }
            }
        }
    }
}

=head3 InsertObject

    my $rows = $erdb->InsertObject($objectType, %fieldHash);

    or

    my $rows = $erdb->InsertObject($objectType, \%fieldHash, %options);

Insert an object into the database. The object is defined by a type name and
then a hash of field names to values. All field values should be
represented by scalars. (Note that for relationships, the primary relation is
the B<only> relation.) Field values for the other relations comprising the
entity are always list references. For example, the following line inserts an
inactive PEG feature named C<fig|188.1.peg.1> with aliases C<ZP_00210270.1> and
C<gi|46206278>.

    $erdb->InsertObject('Feature', id => 'fig|188.1.peg.1', active => 0,
                        feature-type => 'peg', alias => ['ZP_00210270.1',
                        'gi|46206278']);

The next statement inserts a C<HasProperty> relationship between feature
C<fig|158879.1.peg.1> and property C<4> with an evidence URL of
C<http://seedu.uchicago.edu/query.cgi?article_id=142>.

    $erdb->InsertObject('HasProperty', 'from-link' => 'fig|158879.1.peg.1',
                        'to-link' => 4,
                        evidence => 'http://seedu.uchicago.edu/query.cgi?article_id=142');


=over 4

=item newObjectType

Type name of the object to insert.

=item fieldHash

Hash of field names to values. The field names should be specified in
L</Standard Field Name Format>. The default object name is the name of the
object being inserted. The values will be encoded for storage by this method.
Note that this can be an inline hash (for backward compatibility) or a hash
reference.

=item options

Hash of insert options. The current list of options is

=over 8

=item ignore (deprecated)

If TRUE, then duplicate-record errors will be suppressed. If the record already exists, the insert
will not take place.

=item dup

If specified, then duplicate-record errors will be suppressed. If C<ignore> is specified, duplicate
records will be discarded. If C<replace> is specified, duplicate records will replace the previous
version.

=item encoded

If TRUE, the fields are presumed to be already encoded for loading.

=back

=item RETURN

Returns the number of rows inserted.

=back

=cut

sub InsertObject {
    # Get the parameters.
    my ($self, $newObjectType, $first, @leftOvers) = @_;
    # Denote that so far we have not inserted anything.
    my $retVal = 0;
    # Create the field hash.
    my ($fieldHash, $options);
    if (ref $first eq 'HASH') {
        $fieldHash = $first;
        $options = { @leftOvers }
    } else {
        $fieldHash = { $first, @leftOvers };
        $options = {}
    }
    # Get the database handle.
    my $dbh = $self->{_dbh};
    # Parse the field hash. We need to strip off the table names and
    # encode the values.
    my %fixedHash = $self->_SingleTableHash($fieldHash, $newObjectType, $options->{encoded});
    # Get the relation descriptor.
    my $relationData = $self->FindRelation($newObjectType);
    # We'll need a list of the fields being inserted, a list of the corresponding
    # values, and a list of fields the user forgot to specify.
    my @fieldNameList = ();
    my @valueList = ();
    my @missing = ();
    # Get the quote character.
    my $q = $self->q;
    # Loop through the fields in the relation.
    for my $fieldDescriptor (@{$relationData->{Fields}}) {
        # Get the field name and save it. Note we need to fix it up so the hyphens
        # are converted to underscores.
        my $fieldName = $fieldDescriptor->{name};
        my $fixedName = _FixName($fieldName);
        # Look for the named field in the incoming structure. As a courtesy to the
        # caller, we accept both the real field name or the fixed-up one.
        if (exists $fixedHash{$fieldName}) {
            # Here we found the field. There is a special case for the ID that
            # we have to check for.
            if (! defined $fixedHash{$fieldName} && $fieldName eq 'id') {
                # This is the special case. The ID is going to be computed at
                # insert time, so we skip it.
            } else {
                # Normal case. Stash it in both lists.
                push @valueList, $fixedHash{$fieldName};
                push @fieldNameList, "$q$fixedName$q";
            }
        } else {
            # Here the field is not present. Check for a default.
            my $default = $self->_Default($newObjectType, $fieldName);
            if (defined $default) {
                # Yes, we have a default. Push it into the two lists.
                push @valueList, $default;
                push @fieldNameList, "$q$fixedName$q";
            } else {
                # No, this field is officially missing.
                push @missing, $fieldName;
            }
        }
    }
    # Only proceed if there are no missing fields.
    if (@missing > 0) {
        Confess("Insert for $newObjectType failed due to missing fields: " .
            join(' ', @missing)) if T(1);
    } else {
        # Build the INSERT statement.
        my $command = "INSERT";
        if ($options->{ignore}) {
            $command = "INSERT IGNORE";
        } elsif ($options->{dup}) {
            if ($options->{dup} eq 'ignore') {
                $command = "INSERT IGNORE";
            } elsif ($options->{dup} eq 'replace') {
                $command = "REPLACE";
            }
        }
        my $statement = "$command INTO $q$newObjectType$q (" . join (', ', @fieldNameList) .
            ") VALUES (";
        # Create a marker list of the proper size and put it in the statement.
        my @markers = ();
        while (@markers < @fieldNameList) { push @markers, '?'; }
        $statement .= join(', ', @markers) . ")";
        # We have the insert statement, so prepare it.
        my $sth = $dbh->prepare_command($statement);
        # Execute the INSERT statement with the specified parameter list.
        $retVal = $sth->execute(@valueList);
        if (!$retVal) {
            my $errorString = $sth->errstr();
            Confess("Error inserting into $newObjectType: $errorString");
        } else {
            # Convert a true 0 to a false 0.
            $retVal = 0 if ($retVal < 1);
        }
    }
    # Did we successfully insert an entity?
    if ($self->IsEntity($newObjectType) && $retVal) {
        # Yes. Check for secondary fields.
        my %fieldTuples = $self->GetSecondaryFields($newObjectType);
        # Loop through them, inserting their values (if any);
        for my $field (keys %fieldTuples) {
            # Get the value.
            my $values = $fieldHash->{$field};
            # Only proceed if it IS there.
            if (defined $values) {
                # Insure we have a list reference.
                if (ref $values ne 'ARRAY') {
                    $values = [$values];
                }
                # Loop through the values, inserting them.
                for my $value (@$values) {
                    $self->InsertValue($fieldHash->{id}, "$newObjectType($field)", $value);
                    $retVal++;
                }
            }
        }
    }
    # Return the number of rows inserted.
    return $retVal;
}

=head3 UpdateEntity

    $erdb->UpdateEntity($entityName, $id, %fields);

or

    my $ok = $erdb->UpdateEntity($entityName, $id, \%fields, $optional);

Update the values of an entity. This is an unprotected update, so it should only be
done if the database resides on a database server.

=over 4

=item entityName

Name of the entity to update. (This is the entity type.)

=item id

ID of the entity to update. If no entity exists with this ID, an error will be thrown.

=item fields

Hash mapping field names to their new values. All of the fields named
must be in the entity's primary relation, and they cannot any of them be the ID field.
Field names should be in the L</Standard Field Name Format>. The default object name in
this case is the entity name.

=item optional

If specified and TRUE, then the update is optional and will return TRUE if successful and FALSE
if the entity instance was not found. If this parameter is present, I<fields> must be a hash
reference and not a raw hash.

=back

=cut

sub UpdateEntity {
    # Get the parameters.
    my ($self, $entityName, $id, $first, @leftovers) = @_;
    # Get the quote character.
    my $q = $self->q;
    # Get the field hash and optional-update flag.
    my ($fields, $optional);
    if (ref $first eq 'HASH') {
        $fields = $first;
        $optional = $leftovers[0];
    } else {
        $fields = { $first, @leftovers };
    }
    # Fix up the field name hash.
    my @fieldList = keys %{$fields};
    # Verify that the fields exist.
    my $checker = $self->GetFieldTable($entityName);
    for my $field (@fieldList) {
        my $normalizedField = $field;
        $normalizedField =~ tr/_/-/;
        if ($normalizedField eq 'id') {
            Confess("Cannot update the ID field for entity $entityName.");
        } elsif ($checker->{$normalizedField}->{relation} ne $entityName) {
            Confess("Cannot find $field in primary relation of $entityName.");
        }
    }
    # Build the SQL statement.
    my @sets = ();
    my @valueList = ();
    for my $field (@fieldList) {
        push @sets, $q . _FixName($field) . $q . " = ?";
        my $value = $self->EncodeField("$entityName($field)", $fields->{$field});
        push @valueList, $value;
    }
    my $command = "UPDATE $q$entityName$q SET " . join(", ", @sets) . " WHERE id = ?";
    # Add the ID to the list of binding values.
    push @valueList, $id;
    # This will be the return value.
    my $retVal = 1;
    # Call SQL to do the work.
    my $rows = $self->{_dbh}->SQL($command, 0, @valueList);
    # Check for errors.
    if ($rows == 0) {
        if ($optional) {
            $retVal = 0;
        } else {
            Confess("Entity $id of type $entityName not found.");
        }
    }
    # Return the success indication.
    return $retVal;
}

=head3 Reconnect

    my $changeCount = $erdb->Reconnect($relName, $linkType, $oldID, $newID);

Move a relationship so it points to a new entity instance. All instances that reference
a specified ID will be updated to specify a new ID.

=over 4

=item relName

Name of the relationship to update.

=item linkType

C<from> to update the from-link. C<to> to update the to-link.

=item oldID

Old ID value to be changed.

=item new ID

New ID value to be substituted for the old one.

=item RETURN

Returns the number of rows updated.

=back

=cut

sub Reconnect {
    # Get the parameters.
    my ($self, $relName, $linkType, $oldID, $newID) = @_;
    # Get the database handle.
    my $dbh = $self->{_dbh};
    # Get the quote character.
    my $q = $self->q;
    # Compute the link name.
    my $linkName = $linkType . "_link";
    # Create the update statement.
    my $stmt = "UPDATE $q$relName$q SET $linkName = ? WHERE $linkName = ?";
    # Apply the update.
    my $retVal = $dbh->SQL($stmt, 0, $newID, $oldID);
    # Return the number of rows changed.
    return $retVal;
}

=head3 MoveEntity

    my $stats = $erdb->MoveEntity($entityName, $oldID, $newID);

Transfer all relationship records pointing to a specified entity instance so they
point to a different entity instance. This requires calling L</Reconnect> on all
the relationships that connect to the entity.

=over 4

=item entityName

Name of the relevant entity type.

=item oldID

ID of the obsolete entity instance. All relationship records containing this ID will be
changed.

=item newID

ID of the new entity instance. The relationship records containing the old ID will have
this ID substituted for it.

=item RETURN

Returns a L<Stats> object describing the updates.

=back

=cut

sub MoveEntity {
    # Get the parameters.
    my ($self, $entityName, $oldID, $newID) = @_;
    # Create the statistics object.
    my $retVal = Stats->new();
    # Find the entity's connecting relationships.
    my ($froms, $tos) = $self->GetConnectingRelationshipData($entityName);
    # Process the relationship directions.
    my %dirHash = (from => $froms, to => $tos);
    for my $dir (keys %dirHash) {
        # Reconnect the relationships in this direction.
        for my $relName (keys %{$dirHash{$dir}}) {
            my $changes = $self->Reconnect($relName, $dir, $oldID, $newID);
            $retVal->Add("$dir-$relName" => $changes);
        }
    }
    # Return the statistics.
    return $retVal;
}

=head3 Delete

    my $stats = $erdb->Delete($entityName, $objectID, %options);

Delete an entity instance from the database. The instance is deleted along with
all entity and relationship instances dependent on it. The definition of
I<dependence> is recursive.

An object is always dependent on itself. An object is dependent if it is a
1-to-many or many-to-many relationship connected to a dependent entity or if it
is the "to" entity connected to a 1-to-many dependent relationship.

The idea here is to delete an entity and everything related to it. Because this
is so dangerous, and option is provided to simply trace the resulting delete
calls so you can verify the action before performing the delete.

=over 4

=item entityName

Name of the entity type for the instance being deleted.

=item objectID

ID of the entity instance to be deleted.

=item options

A hash detailing the options for this delete operation.

=item RETURN

Returns a statistics object indicating how many records of each particular table were
deleted.

=back

The permissible options for this method are as follows.

=over 4

=item keepRoot

If TRUE, then the entity instances will not be deleted, only the dependent
records.

=item onlyRoot

If TRUE, then the entity instance will be deleted, but none of the attached
data will be removed (the opposite of C<keepRoot>).

=back

=cut

sub Delete {
    # Get the parameters.
    my ($self, $entityName, $objectID, %options) = @_;
    # Declare the return variable.
    my $retVal = Stats->new();
    # Get the quote character.
    my $q = $self->q;
    # Get the crossings table.
    my $crossingTable = $self->{_metaData}{CrossingTable};
    # Encode the object ID.
    my $idParameter = $self->EncodeField("$entityName(id)", $objectID);
    # Get the DBtk object.
    my $db = $self->{_dbh};
    # We're going to generate all the paths branching out from the starting
    # entity. One of the things we have to be careful about is preventing loops.
    # We'll use a hash to determine if we've hit a loop.
    my %alreadyFound = ($entityName => 1);
    # This next list will contain the paths to delete. It is actual a list of lists.
    # Each path is stored in the position determined by its length. We will process
    # them from longest to shortest.
    my @pathLists = ();
    # This final list is used to remember what work still needs to be done. We
    # push paths onto the list, then pop them off to extend the paths. We prime
    # it with the starting point. Note that we will work hard to insure that the
    # last item on a path in the to-do list is always an entity.
    my @todoList = ([$entityName]);
    while (@todoList) {
        # Get the current path tuple.
        my $current = pop @todoList;
        # Stack it as a deletion request.
        my $spLen = scalar @$current;
        push @{$pathLists[$spLen]}, $current;
        # Copy it into a list.
        my @stackedPath = @$current;
        # Pull off the last item on the path. It will always be an entity.
        my $myEntityName = pop @stackedPath;
        # Now we need to look for relationships connected to this entity. We skip
        # this if "onlyRoot" is specified.
        if (! $options{onlyRoot}) {
            # Get the crossings table for this entity.
            my $crossings = $crossingTable->{$myEntityName};
            # Find the relationship that got us here. We don't want to go back.
            my $reverseRel = '';
            if (scalar @stackedPath) {
                $reverseRel = $stackedPath[$#stackedPath];
            }
            # Loop through the crossings. They will all be relationships.
            for my $crossingRel (keys %$crossings) {
                # Get this relationship's descriptor.
                my $relData = $self->FindRelationship($crossingRel);
                # Are we backtracking?
                if ($reverseRel ne $relData->{obverse} && $reverseRel ne $relData->{converse}) {
                    # No. Form a new path for this crossing.
                    my @newPath = (@stackedPath, $myEntityName, $crossingRel);
                    my $newPathLen = scalar @newPath;
                    # Push it into the path lists.
                    push @{$pathLists[$newPathLen]}, \@newPath;
                    # Are we going in the from-direction and is this relationship
                    # 1-to-many and tight?
                    if ($relData->{obverse} eq $crossingRel && $relData->{arity} eq '1M'
                        && ! $relData->{loose}) {
                        # Yes. Check the entity on the other end.
                        my $target = $relData->{to};
                        if (! $alreadyFound{$target}) {
                            # It's new. Stack it for future processing.
                            push @todoList, [@newPath, $target];
                            $alreadyFound{$target} = 1;
                        }
                    }
                }
            }
        }
    }
    # Loop through the path lists in reverse order.
    while (scalar @pathLists) {
        my $pathListSet = pop @pathLists;
        if ($pathListSet) {
            for my $path (@$pathListSet) {
                # Set up for the delete.
                my $pathThing = ERDBtk::Helpers::ObjectPath->new($self, @$path);
                my ($target) = $pathThing->lastObject();
                # Execute the deletion.
                my $count = $pathThing->Delete("$entityName(id) = ?", [$idParameter]);
                # Accumulate the statistics for this delete. The only rows deleted
                # are from the target table, so we use its name to record the
                # statistic.
                $retVal->Add("delete-$target", $count);
            }
        }
    }
    # Return the result.
    return $retVal;
}

=head3 Disconnect

    my $count = $erdb->Disconnect($relationshipName, $originEntityName, $originEntityID);

Disconnect an entity instance from all the objects to which it is related via
a specific relationship. This will delete each relationship instance that
connects to the specified entity.

=over 4

=item relationshipName

Name of the relationship whose instances are to be deleted.

=item originEntityName

Name of the entity that is to be disconnected.

=item originEntityID

ID of the entity that is to be disconnected.

=item RETURN

Returns the number of rows deleted.

=back

=cut

sub Disconnect {
    # Get the parameters.
    my ($self, $relationshipName, $originEntityName, $originEntityID) = @_;
    # Initialize the return count.
    my $retVal = 0;
    # Get the quote character.
    my $q = $self->q;
    # Encode the entity ID.
    my $idParameter = $self->EncodeField("$originEntityName(id)", $originEntityID);
    # Get the relationship descriptor.
    my $structure = $self->_GetStructure($relationshipName);
    # Insure we have a relationship.
    if (! exists $structure->{from}) {
        Confess("$relationshipName is not a relationship in the database.");
    } else {
        # Get the database handle.
        my $dbh = $self->{_dbh};
        # We'll set this value to 1 if we find our entity.
        my $found = 0;
        # Loop through the ends of the relationship.
        for my $dir ('from', 'to') {
            if ($structure->{$dir} eq $originEntityName) {
                $found = 1;
                # Here we want to delete all relationship instances on this side of the
                # entity instance.
                # We do this delete in batches to keep it from dragging down the
                # server.
                my $limitClause = ($ERDBtkExtras::delete_limit ? "LIMIT $ERDBtkExtras::delete_limit" : "");
                my $done = 0;
                while (! $done) {
                    # Do the delete.
                    my $rows = $dbh->SQL("DELETE FROM $q$relationshipName$q WHERE ${dir}_link = ? $limitClause", 0, $idParameter);
                    $retVal += $rows;
                    # See if we're done. We're done if no rows were found or the delete is unlimited.
                    $done = ($rows == 0 || ! $limitClause);
                }
            }
        }
        # Insure we found the entity on at least one end.
        if (! $found) {
            Confess("Entity \"$originEntityName\" does not use $relationshipName.");
        }
        # Return the count.
        return $retVal;
    }
}

=head3 DeleteRow

    $erdb->DeleteRow($relationshipName, $fromLink, $toLink, \%values);

Delete a row from a relationship. In most cases, only the from-link and to-link are
needed; however, for relationships with intersection data values can be specified
for the other fields using a hash.

=over 4

=item relationshipName

Name of the relationship from which the row is to be deleted.

=item fromLink

ID of the entity instance in the From direction.

=item toLink

ID of the entity instance in the To direction.

=item values

Reference to a hash of other values to be used for filtering the delete.

=back

=cut

sub DeleteRow {
    # Get the parameters.
    my ($self, $relationshipName, $fromLink, $toLink, $values) = @_;
    # Get the quote character.
    my $q = $self->q;
    # Create a hash of all the filter information.
    my %filter = ('from-link' => $fromLink, 'to-link' => $toLink);
    if (defined $values) {
        for my $key (keys %{$values}) {
            $filter{$key} = $values->{$key};
        }
    }
    # Build an SQL statement out of the hash.
    my @filters = ();
    my @parms = ();
    for my $key (keys %filter) {
        my ($keyTable, $keyName) = ERDBtk::ParseFieldName($key, $relationshipName);
        push @filters, $q . _FixName($keyName) . $q . " = ?";
        push @parms, $self->EncodeField("$keyTable($keyName)", $filter{$key});
    }
    my $command = "DELETE FROM $q$relationshipName$q WHERE " .
                  join(" AND ", @filters);
    # Execute it.
    my $dbh = $self->{_dbh};
    $dbh->SQL($command, undef, @parms);
}

=head3 DeleteLike

    my $deleteCount = $erdb->DeleteLike($relName, $filter, \@parms);

Delete all the relationship rows that satisfy a particular filter condition.
Unlike a normal filter, only fields from the relationship itself can be used.

=over 4

=item relName

Name of the relationship whose records are to be deleted.

=item filter

A filter clause for the delete query. See L</Filter Clause>.

=item parms

Reference to a list of parameters for the filter clause. See L</Parameter List>.

=item RETURN

Returns a count of the number of rows deleted.

=back

=cut

sub DeleteLike {
    # Get the parameters.
    my ($self, $objectName, $filter, $parms) = @_;
    # Declare the return variable.
    my $retVal;
    # Insure the parms argument is an array reference if the caller left it off.
    if (! defined($parms)) {
        $parms = [];
    }
    # Insure we have a relationship. The main reason for this is if we delete an entity
    # instance we have to yank out a bunch of other stuff with it.
    if ($self->IsEntity($objectName)) {
        Confess("Cannot use DeleteLike on $objectName, because it is not a relationship.");
    } else {
        # Create the SQL command suffix to get the desierd records.
        my $sqlHelper = ERDBtk::Helpers::SQLBuilder->new($self, $objectName);
        my $suffix = $sqlHelper->SetFilterClause($filter);
        # Convert it to a DELETE command.
        my $command = "DELETE $suffix";
        # Execute the command.
        my $dbh = $self->{_dbh};
        my $result = $dbh->SQL($command, 0, @{$parms});
        # Check the results. Note we convert the "0D0" result to a real zero.
        # A failure causes an abnormal termination, so the caller isn't going to
        # worry about it.
        if (! defined $result) {
            Confess("Error deleting from $objectName: " . $dbh->errstr());
        } elsif ($result == 0) {
            $retVal = 0;
        } else {
            $retVal = $result;
        }
    }
    # Return the result count.
    return $retVal;
}

=head3 DeleteValue

    my $numDeleted = $erdb->DeleteValue($entityName, $id, $fieldName, $fieldValue);

Delete secondary field values from the database. This method can be used to
delete all values of a specified field for a particular entity instance, or only
a single value.

Secondary fields are stored in two-column relations separate from an entity's
primary table, and as a result a secondary field can legitimately have no value
or multiple values. Therefore, it makes sense to talk about deleting secondary
fields where it would not make sense for primary fields.

=over 4

=item id

ID of the entity instance to be processed. If the instance is not found, this
method will have no effect. If C<undef> is specified, all values for all of
the entity instances will be deleted.

=item fieldName

Name of the field whose values are to be deleted, in L</Standard Field Name Format>.

=item fieldValue (optional)

Value to be deleted. If not specified, then all values of the specified field
will be deleted for the entity instance. If specified, then only the values
which match this parameter will be deleted.

=item RETURN

Returns the number of rows deleted.

=back

=cut

sub DeleteValue {
    # Get the parameters.
    my ($self, $entityName, $id, $fieldName, $fieldValue) = @_;
    # Get the quote character.
    my $q = $self->q;
    # Declare the return value.
    my $retVal = 0;
    # We need to set up an SQL command to do the deletion. First, we
    # find the name of the field's relation.
    my $table = $self->GetFieldTable($entityName);
    # Now we need some data about this field.
    my $field = $table->{$fieldName};
    my $relation = $field->{relation};
    # Make sure this is a secondary field.
    if ($relation eq $entityName) {
        Confess("Cannot delete values of $fieldName for $entityName.");
    } else {
        # Set up the SQL command to delete all values.
        my $sql = "DELETE FROM $q$relation$q";
        # Build the filter.
        my @filters = ();
        my @parms = ();
        # Check for a filter by ID.
        if (defined $id) {
            push @filters, "id = ?";
            push @parms, $self->EncodeField("$entityName(id)", $id);
        }
        # Check for a filter by value.
        if (defined $fieldValue) {
            push @filters, $q . _FixName($fieldName) . $q . " = ?";
            push @parms, encode($field->{type}, $fieldValue);
        }
        # Append the filters to the command.
        if (@filters) {
            $sql .= " WHERE " . join(" AND ", @filters);
        }
        # Execute the command.
        my $dbh = $self->{_dbh};
        $retVal = $dbh->SQL($sql, 0, @parms);
    }
    # Return the result.
    return $retVal;
}


=head2 Virtual Methods

=head3 CleanKeywords

    my $cleanedString = $erdb->CleanKeywords($searchExpression);

Clean up a search expression or keyword list. This is a virtual method that may
be overridden by the subclass. The base-class method removes extra spaces
and converts everything to lower case.

=over 4

=item searchExpression

Search expression or keyword list to clean. Note that a search expression may
contain boolean operators which need to be preserved. This includes leading
minus signs.

=item RETURN

Cleaned expression or keyword list.

=back

=cut

sub CleanKeywords {
    # Get the parameters.
    my ($self, $searchExpression) = @_;
    # Lower-case the expression and copy it into the return variable. Note that we insure we
    # don't accidentally end up with an undefined value.
    my $retVal = lc($searchExpression || "");
    # Remove extra spaces.
    $retVal =~ s/\s+/ /g;
    $retVal =~ s/(^\s+)|(\s+$)//g;
    # Return the result.
    return $retVal;
}

=head3 PreferredName

    my $name = $erdb->PreferredName();

Return the variable name to use for this database when generating code. The default
is C<erdb>.

=cut

sub PreferredName {
    return 'erdb';
}

=head3 LoadDirectory

    my $dirName = $erdb->LoadDirectory();

Return the name of the directory in which load files are kept. The default is
the FIG temporary directory, which is a really bad choice, but it's always there.

=cut

sub LoadDirectory {
    # Get the parameters.
    my ($self) = @_;
    # Return the directory name.
    return $self->{loadDirectory} || $ERDBtkExtras::temp;
}

=head2 Internal Utility Methods

=head3 _FieldString

    my $fieldString = $erdb->_FieldString($descriptor);

Compute the definition string for a particular field from its descriptor
in the relation table.

=over 4

=item descriptor

Field descriptor containing the field's name and type.

=item RETURN

Returns the SQL declaration string for the field.

=back

=cut

sub _FieldString {
    # Get the parameters.
    my ($self, $descriptor) = @_;
    # Get the quote character.
    my $q = $self->q;
    # Get the fixed-up name.
    my $fieldName = _FixName($descriptor->{name});
    # Compute the SQL type.
    my $fieldType = $self->_TypeString($descriptor);
    # Check for nulls. We need to insure that the field is null-capable if it
    # specifies nulls and that the nullability flag is prepared for the
    # declaration.
    my $nullFlag = "NOT NULL";
    if ($descriptor->{null}) {
        $nullFlag = "";
        if (! $TypeTable->{$descriptor->{type}}->nullable()) {
            Confess("Invalid DBD: field \"$fieldName\" is null, but not of a nullable type.");
        }
    }
    # Assemble the result.
    my $retVal = "$q$fieldName$q $fieldType $nullFlag";
    # Return the result.
    return $retVal;
}

=head3 _TypeString

    my $typeString = $erdb->_TypeString($descriptor);

Determine the SQL type corresponding to a field from its descriptor in the
relation table.

=over 4

=item descriptor

Field descriptor containing the field's name and type.

=item RETURN

Returns the SQL type string for the field.

=back

=cut

sub _TypeString {
    # Get the parameters.
    my ($self, $descriptor) = @_;
    # Compute the SQL type.
    my $typeDescriptor = $TypeTable->{$descriptor->{type}};
    my $retVal = $typeDescriptor->sqlType($self->{_dbh});
    # Return it.
    return $retVal;
}

=head3 _Default

    my $defaultValue = $self->_Default($objectName, $fieldName);

Return the default value for the specified field in the specified object.
If no default value is specified, an undefined value will be returned.

=over 4

=item objectName

Name of the object containing the field.

=item fieldName

Name of the field whose default value is desired.

=item RETURN

Returns the default value for the specified field, or an undefined value if
no default is available.

=back

=cut

sub _Default {
    # Get the parameters.
    my ($self, $objectName, $fieldName) = @_;
    # Declare the return variable.
    my $retVal;
    # Get the field descriptor.
    my $fieldTable = $self->GetFieldTable($objectName);
    my $fieldData = $fieldTable->{$fieldName};
    # Check for a default value. The default value is already encoded,
    # so no conversion is required.
    if (exists $fieldData->{default}) {
        $retVal = $fieldData->{default};
    } else {
        # No default for the field, so get the default for the type.
        # This will be undefined if the type has no default, either.
        $retVal = TypeDefault($fieldData->{type});
    }
    # Return the result.
    return $retVal;
}


=head3 _SingleTableHash

    my %fixedHash = $self->_SingleTableHash($fieldHash, $objectName, $unchanged);

Convert a hash of field names in L</Standard Field Name Format> to field values
into a hash of simple field names to encoded values. This is a common
utility function performed by most update-related methods.

=over 4

=item fieldHash

A hash mapping field names to values. The field names must be in
L</Standard Field Name Format> and must all belong to the same table.

=item objectName

The default object name to be used when no object name is specified for
the field.

=item unchanged

If TRUE, the field values will not be encoded for storage. (It is presumed they already are.) The default is FALSE.

=item RETURN

Returns a hash of simple field names to encoded values for those fields.

=back

=cut

sub _SingleTableHash {
    # Get the parameters.
    my ($self, $fieldHash, $objectName, $unchanged) = @_;
    # Declare the return variable.
    my %retVal;
    # Loop through the fields.
    for my $key (keys %$fieldHash) {
        my $fieldData = $self->_FindField($key, $objectName);
        my $value = $fieldHash->{$key};
        if (! $unchanged) {
            $value = encode($fieldData->{type}, $value);
        }
        $retVal{$fieldData->{realName}} = $value;
    }
    # Return the result.
    return %retVal;
}


=head3 _FindField

    my $fieldData = $erdb->_FindField($string, $defaultName);

Return the descriptor for the named field. If the field does not exist or
the name is invalid, an error will occur.

=over 4

=item string

Field name string to be parsed. See L</Standard Field Name Format>.

=item defaultName (optional)

Default object name to be used if the object name is not specified in the
input string.

=item RETURN

Returns the descriptor for the specified field.

=back

=cut

sub _FindField {
    # Get the parameters.
    my ($self, $string, $defaultName) = @_;
    # Declare the return variable.
    my $retVal;
    # Parse the string.
    my ($tableName, $fieldName) = ERDBtk::ParseFieldName($string, $defaultName);
    if (! defined $tableName) {
        # Here the field name string has an invalid format.
        Confess("Invalid field name specification \"$string\".");
    } else {
        # Find the structure for the specified object.
        $retVal = $self->_CheckField($tableName, $fieldName);
        if (! defined $retVal) {
            Confess("Field \"$fieldName\" not found in \"$tableName\".");
        }
    }
    # Return the result.
    return $retVal;
}

=head3 _CheckField

    my $descriptor = $erdb->_CheckField($objectName, $fieldName);

Return the descriptor for the specified field in the specified entity or
relationship, or an undefined value if the field does not exist.

=over 4

=item objectName

Name of the relevant entity or relationship. If the object does not exist,
an error will be thrown.

=item fieldName

Name of the relevant field.

=item RETURN

Returns the field descriptor from the metadata, or C<undef> if the field
does not exist.

=back

=cut

sub _CheckField {
    # Get the parameters.
    my ($self, $objectName, $fieldName) = @_;
    # Declare the return variable.
    my $retVal;
        # Find the structure for the specified object. This will fail
        # if the object name is invalid.
        my $objectData = $self->_GetStructure($objectName);
        # Look for the field.
        my $fields = $objectData->{Fields};
        if (exists $fields->{$fieldName}) {
            # We found it, so return the descriptor.
            $retVal = $fields->{$fieldName};
        }
    # Return the result.
    return $retVal;
}

=head3 _RelationMap

    my @relationMap = _RelationMap($mappedNameHashRef, $mappedNameListRef);

Create the relation map for an SQL query. The relation map is used by
L</ERDBtk::Object> to determine how to interpret the results of the query.

=over 4

=item mappedNameHashRef

Reference to a hash that maps object name aliases to real object names.

=item mappedNameListRef

Reference to a list of object name aliases in the order they appear in the
SELECT list.

=item RETURN

Returns a list of 3-tuples. Each tuple consists of an object name alias followed
by the actual name of that object and a flag that is TRUE if the alias is a converse.
This enables the L</ERDBtk::Object> to determine the order of the tables in the
query and which object name belongs to each object alias name. Most of the time
the object name and the alias name are the same; however, if an object occurs
multiple times in the object name list, the second and subsequent occurrences
may be given a numeric suffix to indicate it's a different instance. In
addition, some relationship names may be specified using their converse name.

=back

=cut

sub _RelationMap {
    # Get the parameters.
    my ($mappedNameHashRef, $mappedNameListRef) = @_;
    # Declare the return variable.
    my @retVal = ();
    # Build the map.
    for my $mappedName (@{$mappedNameListRef}) {
        push @retVal, [$mappedName, @{$mappedNameHashRef->{$mappedName}}];
    }
    # Return it.
    return @retVal;
}


=head3 _AnalyzeObjectName

    my ($tableName, $embedFlag) = $erdb->_AnalyzeObjectName($baseName);

This method looks at an object name (with the suffix removed) and
determines which table contains it and whether or not it is embedded.
This information is used to determine how to access the object when
forming queries.

=over 4

=item baseName

The relevant object name from an L<Object Name List>, without the numeric'
suffix.

=item RETURN

Returns a two-element list consisting of (0) the name of the table containing
the object and (1) a flag that is TRUE if the object is embedded and FALSE
otherwise.

=back

=cut

sub _AnalyzeObjectName {
    # Get the parameters.
    my ($self, $baseName) = @_;
    # Denote that so far it appears we are not embedded.
    my $embedFlag = 0;
    # Get the alias of the object name.
    my $tableName = $self->{_metaData}{AliasTable}{$baseName};
    if (! $tableName) {
        Confess("Unknown object name $baseName");
    } elsif ($tableName ne $baseName) {
        # Here we must have a relationship. Is it embedded?
        my $relThing = $self->FindRelationship($baseName);
        if ($relThing->{embedded}) {
            $embedFlag = 1;
        }
    }
    # Return the results.
    return ($tableName, $embedFlag);
}


=head3 _GetCrossing

    my $joinList = $erdb->_GetCrossing($table1, $table2);

Return the list of join instructions for crossing from one table to
another, or C<undef> if no crossing is possible.

=over 4

=item table1

Name of the table on which the crossing starts.

=item table2

Name of the table on which the crossing ends.

=item RETURN

Returns a reference to a list of join instructions. Each join instruction will be a 4-tuple containins a source table name,
a source field name, a target table name, and a target field name. Sometimes the referenced list will be empty, indicating
the two tables are the same and no join is required. If no crossing is possible, this method will return C<undef>.

=back

=cut

sub _GetCrossing {
    # Get the parameters.
    my ($self, $table1, $table2) = @_;
    # Get the crossing information. It's in a two-dimensional hash.
    my $retVal = $self->{_metaData}{CrossingTable}{$table1}{$table2};
    # Return the result.
    return $retVal;
}


=head3 _GetStatementHandle

    my $sth = $erdb->_GetStatementHandle($command, $params);

This method will prepare and execute an SQL query, returning the statement handle.
The main reason for doing this here is so that everybody who does SQL queries gets
the benefit of tracing.

=over 4

=item command

Command to prepare and execute.

=item params

Reference to a list of the values to be substituted in for the parameter marks.

=item RETURN

Returns a prepared and executed statement handle from which the caller can extract
results.

=back

=cut

sub _GetStatementHandle {
    # Get the parameters.
    my ($self, $command, $params) = @_;
    Confess("Invalid parameter list.") if (! defined($params) || ref($params) ne 'ARRAY');
    # Trace the query.
    # Get the database handle.
    my $dbh = $self->{_dbh};
    # Prepare the command.
    my $retVal = $dbh->prepare_command($command);
    # Execute it with the parameters bound in. This may require multiple retries.
    my $rv = $retVal->execute(@$params);
    # The number of retries will be counted in here.
    my $retries = 0;
    while (! $rv) {
        # Get the error message.
        my $msg = $dbh->ErrorMessage($retVal);
        # Is a retry worthwhile?
        if ($retries >= $ERDBtkExtras::query_retries) {
            # No, we've tried too many times.
            Confess($msg);
        } elsif ($msg =~ /^DBServer Error/) {
            # Yes. Wait, then try reconnecting.
            sleep($ERDBtkExtras::sleep_time);
            $dbh->Reconnect();
            # Try executing the statement again.
            $retVal = $dbh->prepare_command($command);
            $rv = $retVal->execute(@$params);
            # Denote we've made another retry.
            $retries++;
        } else {
            # No. This error cannot be recovered by reconnecting.
            Confess($msg);
        }
    }
    # Return the statement handle.
    return $retVal;
}

=head3 _GetLoadStats

    my $stats = ERDBtk::_GetLoadStats();

Return a blank statistics object for use by the load methods.

=cut

sub _GetLoadStats{
    return Stats->new();
}

=head3 _DumpRelation

    my $count = $erdb->_DumpRelation($outputDirectory, $relationName);

Dump the specified relation to the specified output file in tab-delimited format.

=over 4

=item outputDirectory

Directory to contain the output file.

=item relationName

Name of the relation to dump.

=item RETURN

Returns the number of records dumped.

=back

=cut

sub _DumpRelation {
    # Get the parameters.
    my ($self, $outputDirectory, $relationName) = @_;
    # Declare the return variable.
    my $retVal = 0;
    # Open the output file.
    my $fileName = "$outputDirectory/$relationName.dtx";
    open(DTXOUT, ">$fileName") || Confess("Could not open dump file $fileName: $!");
    # Create a query for the specified relation.
    my $dbh = $self->{_dbh};
    my $q = $self->q;
    my $query = $dbh->prepare_command("SELECT * FROM $q$relationName$q");
    # Execute the query.
    $query->execute() || Confess("SELECT error dumping $relationName.");
    # Loop through the results.
    while (my @row = $query->fetchrow) {
        # Escape any tabs or new-lines in the row text, and convert NULLs.
        for my $field (@row) {
            if (! defined $field) {
                $field = "\\N";
            } else {
                $field =~ s/\n/\\n/g;
                $field =~ s/\t/\\t/g;
            }
        }
        # Tab-join the row and write it to the output file.
        my $rowText = join("\t", @row);
        print DTXOUT "$rowText\n";
        $retVal++;
    }
    # Close the output file.
    close DTXOUT;
    # Return the write count.
    return $retVal;
}

=head3 _GetStructure

    my $objectData = $self->_GetStructure($objectName);

Get the data structure for a specified entity or relationship.

=over 4

=item objectName

Name of the desired entity or relationship.

=item RETURN

The descriptor for the specified object.

=back

=cut

sub _GetStructure {
    # Get the parameters.
    my ($self, $objectName) = @_;
    # Get the metadata structure.
    my $metadata = $self->{_metaData};
    # Get the descriptor from the metadata.
    my $retVal = $metadata->{Entities}{$objectName};
    if (! $retVal) {
        my $obverse = $metadata->{ConverseTable}{$objectName} // $objectName;
        $retVal = $metadata->{Relationships}{$obverse};
        if (! $retVal) {
            Confess("Object $objectName not found in database.");
        }
    }
    # Return the descriptor.
    return $retVal;
}


=head3 _GetRelationTable

    my $relHash = $erdb->_GetRelationTable($objectName);

Get the list of relations for a specified entity or relationship.

=over 4

=item objectName

Name of the desired entity or relationship.

=item RETURN

A table containing the relations for the specified object.

=back

=cut

sub _GetRelationTable {
    # Get the parameters.
    my ($self, $objectName) = @_;
    # Get the descriptor from the metadata.
    my $objectData = $self->_GetStructure($objectName);
    # Return the object's relation list.
    return $objectData->{Relations};
}

=head3 _ValidateFieldNames

    $erdb->ValidateFieldNames($metadata);

Determine whether or not the field names in the specified metadata
structure are valid. If there is an error, this method will abort.

=over 4

=item metadata

Metadata structure loaded from the XML data definition.

=back

=cut

sub _ValidateFieldNames {
    # Get the object.
    my ($metadata) = @_;
    # Declare the return value. We assume success.
    my $retVal = 1;
    # Loop through the sections of the database definition.
    for my $section ('Entities', 'Relationships') {
        # Loop through the objects in this section.
        for my $object (values %{$metadata->{$section}}) {
            # Loop through the object's fields.
            for my $fieldName (keys %{$object->{Fields}}) {
                # If this field name is invalid, set the return value to zero
                # so we know we encountered an error.
                if (! ValidateFieldName($fieldName)) {
                    $retVal = 0;
                }
            }
        }
    }
    # If an error was found, fail.
    if ($retVal  == 0) {
        Confess("Errors found in field names.");
    }
}

=head3 _LoadRelation

    my $stats = $erdb->_LoadRelation($directoryName, $relationName, $rebuild);

Load a relation from the data in a tab-delimited disk file. The load will only
take place if a disk file with the same name as the relation exists in the
specified directory.

=over 4

=item dbh

DBtk object for accessing the database.

=item directoryName

Name of the directory containing the tab-delimited data files.

=item relationName

Name of the relation to load.

=item rebuild

TRUE if the table should be dropped and re-created before loading.

=item RETURN

Returns a statistical object describing the number of records read and a list of
error messages.

=back

=cut

sub _LoadRelation {
    # Get the parameters.
    my ($self, $directoryName, $relationName, $rebuild) = @_;
    # Create the file name.
    my $fileName = "$directoryName/$relationName";
    # If the file doesn't exist, try adding the .dtx suffix.
    if (! -e $fileName) {
        $fileName .= ".dtx";
        if (! -e $fileName) {
            $fileName = "";
        }
    }
    # Create the return object.
    my $retVal = _GetLoadStats();
    # If a file exists to load the table, its name will be in $fileName. Otherwise, $fileName will
    # be a null string.
    if ($fileName ne "") {
        # Load the relation from the file.
        $retVal = $self->LoadTable($fileName, $relationName, truncate => $rebuild);
    } elsif ($rebuild) {
        # Here we are rebuilding, but no file exists, so we just re-create the table.
        $self->CreateTable($relationName);
    }
    # Return the statistics from the load.
    return $retVal;
}


=head3 _LoadMetaData

    my $metadata = ERDBtk::_LoadMetaData($self, $filename, $external);

This method loads the data describing this database from an XML file into a
metadata structure. The resulting structure is a set of nested hash tables
containing all the information needed to load or use the database. The schema
for the XML file is F<ERDatabase.xml>.

=over 4

=item self

Blessed ERDBtk object.

=item filename

Name of the file containing the database definition.

=item external (optional)

If TRUE, then the internal DBD stored in the database (if any) will be
bypassed. This option is usually used by the load-related command-line
utilities.

=item RETURN

Returns a structure describing the database.

=back

=cut

sub _LoadMetaData {
    # Get the parameters.
    my ($self, $filename, $external) = @_;
    # Declare the return variable.
    my $metadata;
    # Get the database handle.
    my $dbh = $self->{_dbh};
    # Check for an internal DBD.
    if (defined $dbh && ! $external) {
        # Check for a metadata table.
        if ($dbh->table_exists(METADATA_TABLE)) {
            # Check for an internal DBD.
            my $rv = $dbh->SQL("SELECT data FROM " . METADATA_TABLE . " WHERE id = ?",
                               0, "DBD");
            if ($rv && scalar @$rv > 0) {
                # Here we found something. The return value is a reference to a
                # list containing a 1-tuple.
                my $frozen = $rv->[0][0];
                ($metadata) = FreezeThaw::thaw($frozen);
            }
        }
    }
    # If we didn't get an internal DBD, read the external one.
    if (! defined $metadata) {
        # Slurp the XML file into a variable. Extensive use of options is used to
        # insure we get the exact structure we want.
        $metadata = ReadMetaXML($filename);
        # Before we go any farther, we need to validate the field and object names.
        # If an error is found, the method below will fail.
        _ValidateFieldNames($metadata);
        # This will map each converse to its base relationship name.
        my %converses;
        # Next we need to create a hash table for finding relations. The entities
        # and relationships are implemented as one or more database relations.
        my %masterRelationTable = ();
        # We also have a table for mapping alias names to object names. This is
        # useful when processing object name lists.
        my %aliasTable = ();
        # This table gives us instructions for crossing from one object to another.
        # For each pair of names that can appear next to each other, we have a 2-tuple
        # containing a field from the left object and a field from the right object.
        # If the two objects are the same, we have an empty string.
        my %crossings = ();
        # Get the entity and relationship lists.
        my $entityList = $metadata->{Entities};
        my $relationshipList = $metadata->{Relationships};
        # Now we need to find the embedded relationships. For each entity, this hash
        # will list its embedded relationships.
        my %embeds;
        for my $relationshipName (keys %$relationshipList) {
            my $relData = $relationshipList->{$relationshipName};
            # Is thie relationship embedded?
            if ($relData->{embedded}) {
                # Yes. Add it to the to-entity's list.
                push @{$embeds{$relData->{to}}}, $relationshipName;
            }
        }
        # Loop through the entities.
        for my $entityName (keys %{$entityList}) {
            my $entityStructure = $entityList->{$entityName};
            #
            # The first step is to fill in all the entity's missing values. For
            # C<Field> elements, the relation name must be added where it is not
            # specified. For relationships, the B<from-link> and B<to-link> fields
            # must be inserted, and for entities an B<id> field must be added to
            # each relation. Finally, each field will have a C<PrettySort> attribute
            # added that can be used to pull the implicit fields to the top when
            # displaying the field documentation and a realName attribute that tells
            # field's name in the SQL database.
            #
            # Fix up this entity.
            _FixupFields($entityStructure, $entityName);
            # Add the ID field.
            _AddField($entityStructure, 'id', { type => $entityStructure->{keyType},
                                                name => 'id',
                                                relation => $entityName,
                                                realName => 'id',
                                                Notes => { content => "Unique identifier for this \[b\]$entityName\[/b\]." },
                                                PrettySort => 0});
            # Now we need to add the special fields for the embedded relationships. Such fields
            # will all have names of the form <relationshipName>_<fieldName>. Since underscores
            # are not legal in field names, this will not cause a conflict.
            my $embedList = $embeds{$entityName} // [];
            for my $embeddedRelationship (@$embedList) {
                # Get the relationship descriptor.
                my $relData = $relationshipList->{$embeddedRelationship};
                # Fix it up.
                _FixupFields($relData, $entityName, $embeddedRelationship);
                # Add its from- and to-link fields.
                _AddFromToFields($relData, $entityList, $entityName);
                # Get the relationship fields.
                my $relFields = $relData->{Fields};
                # Fix the real names on the from- and to-links.
                my $fromName = join("_", $embeddedRelationship, 'link');
                $relFields->{'from-link'}{realName} = $fromName;
                $relFields->{'to-link'}{realName} = 'id';
                # Add its crossings to the crossings table.
                my $fromEntity = $relData->{from};
                my $converse = $relData->{converse};
                $crossings{$fromEntity}{$embeddedRelationship} = ['id', $fromName];
                $crossings{$converse}{$fromEntity} = [$fromName, 'id'];
                $crossings{$embeddedRelationship}{$entityName} = '';
                $crossings{$entityName}{$converse} = '';
                # Copy the fields (except the to-link) to this entity and mark them imported.
                for my $fieldName (keys %$relFields) {
                    if ($fieldName ne 'to-link') {
                        my %fieldData = %{$relFields->{$fieldName}};
                        $fieldData{imported} = 1;
                        my $myFieldName = $fieldData{realName};
                        _AddField($entityStructure, $myFieldName, \%fieldData);
                    }
                }
                # Add the from-index as an index on this entity.
                my $newIndex = $relData->{FromIndex};
                if (! $newIndex) {
                    $newIndex = { IndexFields => [], Notes => { content =>
                        "This index implements the \[b\]\[link #$embeddedRelationship\]$embeddedRelationship\[/link\]\[/b\] relationship."
                    }};
                } else {
                    # Now we have to map the relationship field names to their real names
                    # in the entity.
                    for my $indexField (@{$newIndex->{IndexFields}}) {
                        my $oldName = $indexField->{name};
                        $indexField->{name} = $relFields->{$oldName}{realName};
                    }
                }
                # The from-link field has to be added at the beginning and the ID at the end.
                unshift @{$newIndex->{IndexFields}}, { name => $fromName, order => 'ascending' };
                push @{$newIndex->{IndexFields}}, { name => 'id', order => 'ascending' };
                # Add the index to our index list.
                push @{$entityStructure->{Indexes}}, $newIndex;
                # Store the relationship and its converse in the alias table.
                $aliasTable{$embeddedRelationship} = $entityName;
                $aliasTable{$converse} = $entityName;
                # Store the converse in the converse table.
                $converses{$converse} = $embeddedRelationship;
                $relData->{obverse} = $embeddedRelationship;
            }
            # Store the entity in the alias table.
            $aliasTable{$entityName} = $entityName;
            #
            # The current field list enables us to quickly find the relation
            # containing a particular field. We also need a list that tells us the
            # fields in each relation. We do this by creating a Relations structure
            # in the entity structure and collating the fields into it based on
            # their C<relation> property. There is one tricky bit, which is that
            # every relation has to have the C<id> field in it. Note also that the
            # field list is put into a C<Fields> member of the relation's structure
            # so that it looks more like the entity and relationship structures.
            #
            # First we need to create the relations list.
            my $relationTable = { };
            # Loop through the fields. We use a list of field names to prevent a problem with
            # the hash table cursor losing its place during the loop.
            my $fieldList = $entityStructure->{Fields};
            my @fieldNames = keys %{$fieldList};
            for my $fieldName (@fieldNames) {
                my $fieldData = $fieldList->{$fieldName};
                # Get the current field's relation name.
                my $relationName = $fieldData->{relation};
                # Insure the relation exists.
                if (!exists $relationTable->{$relationName}) {
                    $relationTable->{$relationName} = { Fields => { } };
                }
                # Add the field to the relation's field structure.
                $relationTable->{$relationName}->{Fields}->{$fieldName} = $fieldData;
            }
            # Now that we've organized all our fields by relation name we need to do
            # some serious housekeeping. We must add the C<id> field to every
            # relation, convert each relation to a list of fields, and add a pointer
            # to the parent entity. First, we need  the ID field itself.
            my $idField = $fieldList->{id};
            # Loop through the relations.
            for my $relationName (keys %{$relationTable}) {
                my $relation = $relationTable->{$relationName};
                # Point this relation to its parent entity.
                $relation->{owner} = $entityName;
                # Get the relation's field list.
                my $relationFieldList = $relation->{Fields};
                # Add the ID field to it. If the field's already there, it will not make any
                # difference.
                $relationFieldList->{id} = $idField;
                # Convert the field set from a hash into a list using the pretty-sort number.
                $relation->{Fields} = _ReOrderRelationTable($relationFieldList);
                # Add the relation to the master table.
                $masterRelationTable{$relationName} = $relation;
            }
            # The indexes come next. The primary relation will have a unique-keyed
            # index based on the ID field. The other relations must have at least
            # one index that begins with the ID field. In addition, the metadata may
            # require alternate indexes. We do those alternate indexes first. To
            # begin, we need to get the entity's field list and index list.
            my $indexList = $entityStructure->{Indexes};
            # Loop through the indexes.
            for my $indexData (@{$indexList}) {
                # We need to find this index's fields. All of them should belong to
                # the same relation. The ID field is an exception, since it's in all
                # relations.
                my $relationName = '0';
                for my $fieldDescriptor (@{$indexData->{IndexFields}}) {
                    # Get this field's name.
                    my $fieldName = $fieldDescriptor->{name};
                    # Only proceed if it is NOT the ID field.
                    if ($fieldName ne 'id') {
                        # Insure the field name is valid.
                        my $fieldThing = $fieldList->{$fieldName};
                        if (! defined $fieldThing) {
                            Confess("Invalid index: field $fieldName does not belong to $entityName.");
                        } else {
                            # Find the relation containing the current index field.
                            my $thisName = $fieldThing->{relation};
                            if ($relationName eq '0') {
                                # Here we're looking at the first field, so we save its
                                # relation name.
                                $relationName = $thisName;
                            } elsif ($relationName ne $thisName) {
                                # Here we have a field mismatch.
                                Confess("Mixed index: field $fieldName does not belong to relation $relationName.");
                            }
                        }
                    }
                }
                # Now $relationName is the name of the relation that contains this
                # index. Add the index structure to the relation.
                push @{$relationTable->{$relationName}->{Indexes}}, $indexData;
            }
            # Now each index has been put in a relation. We need to add the primary
            # index for the primary relation.
            push @{$relationTable->{$entityName}->{Indexes}},
                { IndexFields => [ {name => 'id', order => 'ascending'} ], primary => 1,
                  Notes => { content => "Primary index for $entityName." }
                };
            # The next step is to insure that each relation has at least one index
            # that begins with the ID field. After that, we convert each relation's
            # index list to an index table. We first need to loop through the
            # relations.
            for my $relationName (keys %{$relationTable}) {
                my $relation = $relationTable->{$relationName};
                # Get the relation's index list.
                my $indexList = $relation->{Indexes};
                # Insure this relation has an ID index.
                my $found = 0;
                for my $index (@{$indexList}) {
                    if ($index->{IndexFields}->[0]->{name} eq "id") {
                        $found = 1;
                    }
                }
                if ($found == 0) {
                    push @{$indexList}, { IndexFields => [ {name => 'id',
                                                            order => 'ascending'} ] };
                }
                # Attach all the indexes to the relation.
                _ProcessIndexes($indexList, $relation, $relationName);
            }
            # Finally, we add the relation structure to the entity.
            $entityStructure->{Relations} = $relationTable;
        }
        # Loop through the relationships. Relationships actually turn out to be much
        # simpler than entities. For one thing, there is only a single constituent
        # relation.
        for my $relationshipName (keys %{$relationshipList}) {
            my $relationshipStructure = $relationshipList->{$relationshipName};
            # Embedded relationships were already handled, so only process this
            # relationship if it is NOT embedded.
            if (! $relationshipStructure->{embedded}) {
                # Fix up this relationship.
                _FixupFields($relationshipStructure, $relationshipName);
                _AddFromToFields($relationshipStructure, $entityList, $relationshipName);
                # Create an index-free relation from the fields.
                my $thisRelation = { Fields => _ReOrderRelationTable($relationshipStructure->{Fields}),
                                     Indexes => { }, owner => $relationshipName };
                $relationshipStructure->{Relations} = { $relationshipName => $thisRelation };
                # Get the converse name.
                my $converse = $relationshipStructure->{converse};
                # Put the relationship in the alias table.
                $aliasTable{$relationshipName} = $relationshipName;
                $aliasTable{$converse} = $relationshipName;
                # Put the converse in the converse table.
                $converses{$converse} = $relationshipName;
                $relationshipStructure->{obverse} = $relationshipName;
                # Add the alternate indexes (if any). This MUST be done before the FROM
                # and TO indexes, because it erases the relation's index list.
                if (exists $relationshipStructure->{Indexes}) {
                    _ProcessIndexes($relationshipStructure->{Indexes}, $thisRelation, $relationshipName);
                }
                # Create the FROM and TO indexes.
                _CreateRelationshipIndex("From", $relationshipName, $relationshipStructure);
                _CreateRelationshipIndex("To", $relationshipName, $relationshipStructure);
                # Add the relation to the master table.
                $masterRelationTable{$relationshipName} = $thisRelation;
                # Compute the crossings.
                my $fromEntity = $relationshipStructure->{from};
                my $toEntity = $relationshipStructure->{to};
                $crossings{$fromEntity}{$relationshipName} = ['id', 'from_link'];
                $crossings{$converse}{$fromEntity} = ['from_link', 'id'];
                $crossings{$relationshipName}{$toEntity} = ['to_link', 'id'];
                $crossings{$toEntity}{$converse} = ['id', 'to_link'];
            }
        }
        # Now loop through the relationships, creating jump-crossings, that is, crossings that skip over intervening
        # entities. We can only do this because the entity between two relationships is unique. To extend
        # the crossings further, we would need to insure the paths are unambiguous.
        for my $relationshipName (keys %{$relationshipList}) {
            # Do the forward direction, then the converse.
            for my $relVersion ($relationshipName, $relationshipList->{$relationshipName}{converse}) {
                my @targets = keys %{$crossings{$relVersion}};
                for my $target (@targets) {
                    # Now we have an entity we can reach from this relationship. Get our half of the crossing.
                    my $crossList = $crossings{$relVersion}{$target};
                    for my $remote (keys %{$crossings{$target}}) {
                         my $remoteList = $crossings{$target}{$remote};
                         # We have four cases, depending on which of the relationships is embedded.
                         # Two of the cases have the same effect.
                         if ($crossList && $remoteList) {
                             # Both relationships are real.
                             $crossings{$relVersion}{$remote} = [$crossList->[0], $remoteList->[1]];
                         } elsif ($crossList) {
                             # Only the crossing is real.
                             $crossings{$relVersion}{$remote} = $crossList;
                         } else {
                             # Either the remote is real and we want to use it, or neither is
                             # real and we want to store a null string. Either way we just copy
                             # the remote.
                             $crossings{$relVersion}{$remote} = $remoteList;
                         }
                    }
                }
            }
        }
        # Now we loop through the entities, creating entity jumps. For each entity, we list the number of ways
        # to get to each other object. If there's only one way, we create a jump.
        my %jumpTable;
        for my $entity (keys %$entityList) {
            my %targets;
            for my $path (keys %{$crossings{$entity}}) {
                for my $target (keys %{$crossings{$path}}) {
                    push @{$targets{$target}}, $path;
                }
            }
            # Only keep jumps that are unambiguous.
            my @targets = keys %targets;
            for my $target (@targets) {
                if (scalar @{$targets{$target}} == 1) {
                    $targets{$target} = $targets{$target}[0];
                } else {
                    delete $targets{$target};
                }
            }
            # Now, if there is only one path from this entity to a particular target, the targets
            # hash will map to its name.
            $jumpTable{$entity} = \%targets;
        }
        # Now store the master relation table, crossing table, converse table, and alias table in the metadata structure.
        $metadata->{RelationTable} = \%masterRelationTable;
        $metadata->{AliasTable} = \%aliasTable;
        $metadata->{CrossingTable} = \%crossings;
        $metadata->{ConverseTable} = \%converses;
        $metadata->{JumpTable} = \%jumpTable;
    }
    # Return the metadata structure.
    return $metadata;
}

=head3 _CreateRelationshipIndex

    ERDBtk::_CreateRelationshipIndex($indexKey, $relationshipName, $relationshipStructure);

Create an index for a relationship's relation.

=over 4

=item indexKey

Type of index: either C<"From"> or C<"To">.

=item relationshipName

Name of the relationship.

=item relationshipStructure

Structure describing the relationship that the index will sort.

=back

=cut

sub _CreateRelationshipIndex {
    # Get the parameters.
    my ($indexKey, $relationshipName, $relationshipStructure) = @_;
    # Get the target relation.
    my $relationStructure = $relationshipStructure->{Relations}->{$relationshipName};
    # Create a descriptor for the link field that goes at the beginning of this
    # index.
    my $firstField = { name => lcfirst $indexKey . '-link', order => 'ascending' };
    # Get the target index descriptor.
    my $newIndex = $relationshipStructure->{$indexKey . "Index"};
    # Add the first field to the index's field list. Due to the craziness of
    # PERL, if the index descriptor does not exist, it will be created
    # automatically so we can add the field to it.
    unshift @{$newIndex->{IndexFields}}, $firstField;
    # If this is a one-to-many relationship, the "To" index is unique. The index
    # can also be forced unique by the user.
    if ($relationshipStructure->{arity} eq "1M" && $indexKey eq "To" ||
        $relationshipStructure->{unique}) {
        $newIndex->{unique} = 1;
    }
    # Add the index to the relation.
    _AddIndex("idx$indexKey$relationshipName", $relationStructure, $newIndex);
}

=head3 _ProcessIndexes

    ERDBtk::_ProcessIndexes($indexList, $relation);

Build the data structures for the specified indexes in the specified relation.

=over 4

=item indexList

Reference to a list of indexes. Each index is a hash reference containing an
optional C<Notes> value that describes the index and an C<IndexFields> value
that is a reference to a list of index field structures. An index field
structure, in turn, is a reference to a hash that contains a C<name> attribute
for the field name and an C<order> attribute that specifies either C<ascending>
or C<descending>. In this sense the index list encapsulates the XML C<Indexes>
structure in the database definition.

=item relation

The structure that describes the current relation. The new index descriptors
will be stored in the structure's C<Indexes> member. Any previous data in the
member will be lost.

=item relName

The name of the relation whose indexes are being processed.

=back

=cut

sub _ProcessIndexes {
    # Get the parameters.
    my ($indexList, $relation, $relName) = @_;
    # Now we need to convert the relation's index list to an index table. We
    # begin by creating an empty table in the relation structure.
    $relation->{Indexes} = { };
    # Loop through the indexes.
    my $count = 0;
    for my $index (@{$indexList}) {
        # We must Add this index to the index table. Compute the index name.
        my $indexName;
        if ($index->{primary}) {
            $indexName = 'PRIMARY';
        } else {
            $indexName = "idx$count$relName";
        }
        _AddIndex($indexName, $relation, $index);
        # Increment the counter so that the next index has a different name.
        $count++;
    }
}

=head3 _AddIndex

    ERDBtk::_AddIndex($indexName, $relationStructure);

Add an index to a relation structure.

This is a static method.

=over 4

=item indexName

Name to give to the new index.

=item relationStructure

Relation structure to which the new index should be added.

=item newIndex

New index to add.

=back

=cut

sub _AddIndex {
    # Get the parameters.
    my ($indexName, $relationStructure, $newIndex) = @_;
    # We want to re-do the index's field list. Instead of an object for each
    # field, we want a string consisting of the field name optionally followed
    # by the token DESC.
    my @fieldList = ( );
    for my $field (@{$newIndex->{IndexFields}}) {
        # Create a string containing the field name.
        my $fieldString = $field->{name};
        # Add the ordering token if needed.
        if ($field->{order} && $field->{order} eq "descending") {
            $fieldString .= " DESC";
        }
        # Push the result onto the field list.
        push @fieldList, $fieldString;
    }
    # Store the field list just created as the new index field list.
    $newIndex->{IndexFields} = \@fieldList;
    # Add the index to the relation's index list.
    $relationStructure->{Indexes}->{$indexName} = $newIndex;
}

=head3 _FixupFields

    ERDBtk::_FixupFields($structure, $defaultRelationName, $objectName);

This method fixes the field list for the metadata of an entity or relationship.
It will add the caller-specified relation name to fields that do not have a name,
the real name to all fields, and set the C<PrettySort> values.

=over 4

=item structure

Entity or relationship structure to be fixed up.

=item defaultRelationName

Default relation name to be added to the fields.

=item objectName

If specified, this is an embedded relationship. The objectName is the
relationship's original name, which is different from the default
relation name.

=back

=cut

sub _FixupFields {
    # Get the parameters.
    my ($structure, $defaultRelationName, $objectName) = @_;
    # Insure the structure has a field list.
    if (!exists $structure->{Fields}) {
        # Here it doesn't, so we create a new one.
        $structure->{Fields} = { };
    } else {
        # Loop through the fields.
        my $fieldStructures = $structure->{Fields};
        for my $fieldName (keys %{$fieldStructures}) {
            my $fieldData = $fieldStructures->{$fieldName};
            # Store the field name so we can find it when we're looking at a descriptor
            # without its key.
            $fieldData->{name} = $fieldName;
            # Get the field type.
            my $type = $fieldData->{type};
            # Validate it.
            if (! exists $TypeTable->{$type}) {
                Confess("Field $fieldName of $defaultRelationName has unknown type \"$type\".");
            }
            # Plug in a relation name if one is missing or this is an embedded relationship.
            if ($objectName || ! exists $fieldData->{relation}) {
                $fieldData->{relation} = $defaultRelationName;
            }
            # Add the PrettySortValue.
            $fieldData->{PrettySort} = $TypeTable->{$type}->prettySortValue();
            # Compute the real name. This consists of the field name in SQL format.
            # If this is an embedded relationship, the original object name is added to the field name.
            my $sqlName = _FixName($fieldName);
            if ($objectName) {
                $sqlName = join('_', $objectName, $sqlName);
            }
            $fieldData->{realName} = $sqlName;
        }
    }
}

=head3 _AddFromToFields

    ERDBtk::_AddFromToFields($relationshipStructure, $entityList, $relationshipName);

Add the from-link and to-link fields to a relationship's field hash.

=over 4

=item relationshipStructure

The relationship structure whose field list needs from- and to-links.

=item entityList

A reference to a hash of the entities in the DBD.

=item relationshipName

The name of the relation containing the fields.

=back

=cut

sub _AddFromToFields{
    # Get the parameters.
    my ($relationshipStructure, $entityList, $relationshipName) = @_;
    # Format a description for the FROM field.
    my $fromEntity = $relationshipStructure->{from};
    my $fromComment = "[b]id[/b] of the source [b][link #$fromEntity]$fromEntity\[/link][/b].";
    # Get the FROM entity's key type.
    my $fromType = $entityList->{$fromEntity}->{keyType};
    # Add the FROM field.
    _AddField($relationshipStructure, 'from-link', { type => $fromType,
                                                name => 'from-link',
                                                relation => $relationshipName,
                                                realName => 'from_link',
                                                Notes => { content => $fromComment },
                                                PrettySort => 1});
    # Format a description for the TO field.
    my $toEntity = $relationshipStructure->{to};
    my $toComment = "[b]id[/b] of the target [b][link #$toEntity]$toEntity\[/link][/b].";
    # Get the TO entity's key type.
    my $toType = $entityList->{$toEntity}->{keyType};
    # Add the TO field.
    _AddField($relationshipStructure, 'to-link', { type=> $toType,
                                              name => 'to-link',
                                              relation => $relationshipName,
                                              realName => 'to_link',
                                              Notes => { content => $toComment },
                                              PrettySort => 1});
}

=head3 _FixName

    my $fixedName = ERDBtk::_FixName($fieldName);

Fix the incoming field name so that it is a legal SQL column name.

=over 4

=item fieldName

Field name to fix.

=item RETURN

Returns the fixed-up field name.

=back

=cut

sub _FixName {
    # Get the parameter.
    my ($fieldName, $converse) = @_;
    # Replace its minus signs with underscores.
    $fieldName =~ s/-/_/g;
    # Return the result.
    return $fieldName;
}

=head3 _SQLFieldName

    my $sqlName = $erdb->_SQLFieldName($baseName, $fieldName);

Compute the real SQL name of the specified field. This method must handle
flipping from-link and to-link on a converse relationship, and it must
translate the field name to its real name.

=over 4

=item baseName

The name of the table containing the field.

=item fieldName

The actual field name itself.

=item RETURN

Returns the SQL name of the field, or C<undef> if the field does
not exist.

=back

=cut

sub _SQLFieldName {
    # Get the parameters.
    my ($self, $baseName, $fieldName) = @_;
    # Declare the return variable.
    my $retVal;
    # We'll compute the real field name in here.
    my $realName = $fieldName;
    # Allow the use of underscores for hyphens.
    $realName =~ tr/_/-/;
    # Is this a converse relationship?
    my $obverse = $self->{_metaData}{ConverseTable}{$baseName};
    if ($obverse) {
        # Yes. Do the from-to flipping.
        if ($fieldName eq 'from-link') {
            $realName = 'to-link';
        } elsif ($fieldName eq 'to-link') {
            $realName = 'from-link';
        }
        # Denote that the object we're looking for is the
        # obverse.
        $baseName = $obverse;
    }
    # Get the object's field table.
    my $fieldTable = $self->GetFieldTable($baseName);
    # Get the field descriptor.
    my $fieldThing = $fieldTable->{$realName};
    if ($fieldThing) {
        # We found the field, so return its real name.
        $retVal = $fieldThing->{realName};
    }
    # Return the result.
    return $retVal;
}

=head3 _FixNames

    my @fixedNames = ERDBtk::_FixNames(@fields);

Fix all the field names in a list. This is essentially a batch call to
L</_FixName>.

=over 4

=item fields

List of field names to fix.

=item RETURN

Returns a list of fixed-up versions of the incoming field names.

=back

=cut

sub _FixNames {
    # Create the result list.
    my @result = ( );
    # Loop through the incoming parameters.
    for my $field (@_) {
        push @result, _FixName($field);
    }
    # Return the result.
    return @result;
}

=head3 _AddField

    ERDBtk::_AddField($structure, $fieldName, $fieldData);

Add a field to a field list.

=over 4

=item structure

Structure (usually an entity or relationship) that is to contain the field.

=item fieldName

Name of the new field.

=item fieldData

Structure containing the data to put in the field.

=back

=cut

sub _AddField {
    # Get the parameters.
    my ($structure, $fieldName, $fieldData) = @_;
    # Create the field structure by copying the incoming data.
    my $fieldStructure = {%{$fieldData}};
    # Get a reference to the field list itself.
    my $fieldList = $structure->{Fields};
    # Add the field to the field list.
    $fieldList->{$fieldName} = $fieldStructure;
}

=head3 _ReOrderRelationTable

    my \@fieldList = ERDBtk::_ReOrderRelationTable(\%relationTable);

This method will take a relation table and re-sort it according to the implicit
ordering of the C<PrettySort> property. Instead of a hash based on field names,
it will return a list of fields. This requires creating a new hash that contains
the field name in the C<name> property but doesn't have the C<PrettySort>
property, and then inserting that new hash into the field list.

This is a static method.

=over 4

=item relationTable

Relation hash to be reformatted into a list.

=item RETURN

A list of field hashes.

=back

=cut

sub _ReOrderRelationTable {
    # Get the parameters.
    my ($relationTable) = @_;
    # Create the return list.
    my @resultList;
    # Rather than copy all the fields in a single pass, we make multiple passes
    # and only copy fields whose PrettySort value matches the current pass
    # number. This process continues until we process all the fields in the
    # relation.
    my $fieldsLeft = (values %{$relationTable});
    for (my $sortPass = 0; $fieldsLeft > 0; $sortPass++) {
        # Loop through the fields. Note that we lexically sort the fields. This
        # makes field name secondary to pretty-sort number in the final
        # ordering.
        for my $fieldName (sort keys %{$relationTable}) {
            # Get this field's data.
            my $fieldData = $relationTable->{$fieldName};
            # Verify the sort pass.
            if ($fieldData->{PrettySort} == $sortPass) {
                # Here we're in the correct pass. Denote we've found a field.
                $fieldsLeft--;
                # The next step is to create the field structure. This done by
                # copying all of the field elements except PrettySort and adding
                # the name.
                my %thisField;
                for my $property (keys %{$fieldData}) {
                    if ($property ne 'PrettySort') {
                        $thisField{$property} = $fieldData->{$property};
                    }
                }
                $thisField{name} = $fieldName;
                # Now we add this field to the end of the result list.
                push @resultList, \%thisField;
            }
        }
    }
    # Return a reference to the result list.
    return \@resultList;

}

=head3 _IsPrimary

    my $flag = $erdb->_IsPrimary($relationName);

Return TRUE if a specified relation is a primary relation, else FALSE. A
relation is primary if it has the same name as an entity or relationship.

=over 4

=item relationName

Name of the relevant relation.

=item RETURN

Returns TRUE for a primary relation, else FALSE.

=back

=cut

sub _IsPrimary {
    # Get the parameters.
    my ($self, $relationName) = @_;
    # Check for the relation in the entity table.
    my $entityTable = $self->{_metaData}{Entities};
    my $retVal = exists $entityTable->{$relationName};
    if (! $retVal) {
        # Check for it in the relationship table.
        my $relationshipTable = $self->{_metaData}{Relationships};
        $retVal = exists $relationshipTable->{$relationName};
    }
    # Return the determination indicator.
    return $retVal;
}


=head3 InternalizeDBD

    $erdb->InternalizeDBD();

Save the DBD metadata into the database so that it can be retrieved in the
future.

=cut

sub InternalizeDBD {
    # Get the parameters.
    my ($self) = @_;
    # Get the database handle.
    my $dbh = $self->{_dbh};
    # Insure we have a metadata table.
    if (! $dbh->table_exists(METADATA_TABLE)) {
        $dbh->create_table(tbl => METADATA_TABLE,
                           flds => 'id VARCHAR(20) NOT NULL PRIMARY KEY, data MEDIUMTEXT');
    }
    # Delete the current DBD record.
    $dbh->SQL("DELETE FROM " . METADATA_TABLE . " WHERE id = ?", 0, 'DBD');
    # Freeze the DBD metadata.
    my $frozen = FreezeThaw::freeze($self->{_metaData});
    # Store it in the database.
    $dbh->SQL("INSERT INTO " . METADATA_TABLE . " (id, data) VALUES (?, ?)", 0, 'DBD',
              $frozen);
}


=head2 Autocounter Support

=head3 RefreshIDTable

    $erdb->RefreshIDTable();

This method insures the ID table is up-to-date. It is dropped and recreated, and
its records are computed from the database content of the autocounter entities.

=cut

sub RefreshIDTable {
    # Get the parameters.
    my ($self) = @_;
    # Get the database handle.
    my $dbh = $self->{_dbh};
    # Drop the table if it exists.
    $dbh->drop_table(tbl => ID_TABLE);
    # Re-create the table.
    $dbh->create_table(tbl => ID_TABLE,
            flds => 'entity VARCHAR(128) NOT NULL PRIMARY KEY, next_id BIGINT');
    # Loop through the entity definitions.
    my $entityHash = $self->{_metaData}{Entities};
    for my $entity (keys %$entityHash) {
        # Is this an autocounter entity?
        if ($entityHash->{$entity}{autocounter}) {
            # Yes. Get its highest key.
            my ($maxID) = $self->GetFlat($entity, "ORDER BY $entity(id) DESC LIMIT 1", [], 'id');
            # Compute the next available key.
            my $nextID = ($maxID ? $maxID + 1 : 1);
            # Create the entity's ID record.
            $dbh->SQL("INSERT INTO " . ID_TABLE . " (entity, next_id) VALUES (?, ?)", 0,
                    $entity, $nextID);
        }
    }
}

=head3 AllocateIds

    my $nextID = $erdb->AllocateIds($entityName, $count);

Allocate one or more autocounter IDs for the specified entity. After calling this
method, the client may freely insert entity instances with ID numbers in the range
[$nextID, $nextID + $count - 1].

=over 4

=item entityName

Name of the entity for which IDs are to be allocated.

=item count

Number of IDs to allocate.

=item RETURN

Returns the next available ID for the named entity.

=back

=cut

sub AllocateIds {
    # Get the parameters.
    my ($self, $entityName, $count) = @_;
    # Get the database handle.
    my $dbh = $self->{_dbh};
    # Loop until we successfully find an ID.
    my $retVal;
    while (! defined $retVal) {
        # Get the next ID for the named entity.
        my $rv = $dbh->SQL("SELECT next_id FROM " . ID_TABLE . " WHERE entity = ?", 0, $entityName);
        # Extract the result record.
        if (! scalar @$rv) {
            Confess("$entityName is not an autocounter entity.");
        } else {
            my $nextID = $rv->[0][0];
            # Attempt to allocate the ID.
            my $success = $dbh->SQL("UPDATE " . ID_TABLE .
                   " SET next_id = ? WHERE entity = ? AND next_id = ?", 0,
                   $nextID + $count, $entityName, $nextID);
            # If we succeeded, return the first known ID.
            if ($success > 0) {
                $retVal = $nextID;
            }
        }
    }
    # Return the new ID allocated.
    return $retVal;
}


=head2 Internal Documentation-Related Methods

Several of these methods refer to a wiki or a wiki rendering object.
There is no longer wiki support; however, the L<ERDBtk::PDocPage>
uses this code to render HTML by supporting wiki-like operations.

=head3 _FindObject

    my $objectData = $erdb->_FindObject($list => $name);

Return the structural descriptor of the specified object (entity,
relationship, or shape), or an undefined value if the object does not
exist.

=over 4

=item list

Name of the list containing the desired type of object (C<Entities>,
C<Relationships>, or C<Shapes>).

=item name

Name of the desired object.

=item RETURN

Returns the object descriptor if found, or C<undef> if the object does
not exist or is not of the proper type.

=back

=cut

sub _FindObject {
    # Get the parameters.
    my ($self, $list, $name) = @_;
    # Declare the return variable.
    my $retVal;
    # If the object exists, return its descriptor.
    my $thingHash = $self->{_metaData}{$list};
    if (exists $thingHash->{$name}) {
        $retVal = $thingHash->{$name};
    }
    # Return the result.
    return $retVal;
}

=head3 ObjectNotes

    my @noteParagraphs = ERDBtk::ObjectNotes($objectData, $wiki);

Return a list of the notes and asides for an entity or relationship in
Wiki format.

=over 4

=item objectData

The metadata for the desired entity or relationship.

=item wiki

Wiki object used to render text.

=item RETURN

Returns a list of text paragraphs in Wiki markup form.

=back

=cut

sub ObjectNotes {
    # Get the parameters.
    my ($objectData, $wiki) = @_;
    # Declare the return variable.
    my @retVal;
    # Loop through the types of notes.
    for my $noteType (qw(Notes Asides)) {
        my $text = $objectData->{$noteType};
        if ($text) {
            push @retVal, _WikiNote($text->{content}, $wiki);
        }
    }
    # Return the result.
    return @retVal;
}

=head3 _WikiNote

    my $wikiText = ERDBtk::_WikiNote($dataString, $wiki);

Convert a note or comment to Wiki text by replacing some bulletin-board codes
with HTML. The codes supported are C<[b]> for B<bold>, C<[i]> for I<italics>,
C<[link]> for links, C<[list]> for bullet lists. and C<[p]> for a new paragraph.
All the codes are closed by slash-codes. So, for example, C<[b]Feature[/b]>
displays the string C<Feature> in boldface.

=over 4

=item dataString

String to convert to Wiki text.

=item wiki

Wiki object used to format the text.

=item RETURN

An Wiki text string derived from the input string.

=back

=cut

sub _WikiNote {
    # Get the parameter.
    my ($dataString, $wiki) = @_;
    # HTML-escape the text.
    my $retVal = CGI::escapeHTML($dataString);
    # Substitute the italic code.
    $retVal =~ s#\[i\](.+?)\[/i\]#$wiki->Italic($1)#sge;
    # Substitute the bold code.
    $retVal =~ s#\[b\](.+?)\[/b\]#$wiki->Bold($1)#sge;
    # Substitute for the paragraph breaks.
    $retVal =~ s#\[p\](.+?)\[/p\]#$wiki->Para($1)#sge;
    # Now we do the links, which are complicated by the need to know two
    # things: the target URL and the text.
    $retVal =~ s#\[link\s+([^\]]+)\]([^\[]+)\[/link\]#$wiki->LinkMarkup($1, $2)#sge;
    # Finally, we have bullet lists.
    $retVal =~ s#\[list\](.+?)\[/list\]#$wiki->List(split /\[\*\]/, $1)#sge;
    # Return the result.
    return $retVal;
}

=head3 _ComputeRelationshipSentence

    my $text = ERDBtk::_ComputeRelationshipSentence($wiki, $relationshipName, $relationshipStructure, $dir);

The relationship sentence consists of the relationship name between the names of
the two related entities and an arity indicator.

=over 4

=item wiki

L<WikiTools> object for rendering links. If this parameter is undefined, no
link will be put in place.

=item relationshipName

Name of the relationship.

=item relationshipStructure

Relationship structure containing the relationship's description and properties.

=item dir (optional)

Starting point of the relationship: C<from> (default) or C<to>.

=item RETURN

Returns a string containing the entity names on either side of the relationship
name and an indicator of the arity.

=back

=cut

sub _ComputeRelationshipSentence {
    # Get the parameters.
    my ($wiki, $relationshipName, $relationshipStructure, $dir) = @_;
    # This will contain the first, second, and third pieces of the sentence.
    my @relWords;
    # Process according to the direction.
    if (! $dir || $dir eq 'from') {
        # Here we're going forward.
        @relWords = ($relationshipStructure->{from}, $relationshipName,
                     $relationshipStructure->{to});
    } else {
        # Here we're going backward.
        my $relName = $relationshipStructure->{converse};
        @relWords = ($relationshipStructure->{to}, $relName,
                     $relationshipStructure->{from});
    }
    # Now we need to set up the link. This is only necessary if the wiki object
    # is defined.
    if (defined $wiki) {
        $relWords[1] = $wiki->LinkMarkup("#$relationshipName", $relWords[1]);
    }
    # Compute the arity.
    my $arityCode = $relationshipStructure->{arity};
    push @relWords, "($ArityTable{$arityCode})";
    # Form the sentence.
    my $retVal = join(" ", @relWords) . ".";
    return $retVal;
}

=head3 _WikiObjectTable

    my $tableMarkup = _WikiObjectTable($name, $fieldStructure, $wiki);

Generate the field table for the named entity or relationship.

=over 4

=item name

Name of the object whose field table is being generated.

=item fieldStructure

Field structure for the object. This is a hash mapping field names to field
data.

=item wiki

L<WikiTools> object (or equivalent) for rendering HTML.

=item RETURN

Returns the markup for a table of field information.

=back

=cut

sub _WikiObjectTable {
    # Get the parameters.
    my ($name, $fieldStructure, $wiki) = @_;
    # Compute the table header row and data rows.
    my ($header, $rows) = ComputeFieldTable($wiki, $name, $fieldStructure);
    # Convert it to a table.
    my $retVal = $wiki->Table($header, @$rows);
    # Return the result.
    return $retVal;
}

1;
