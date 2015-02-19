use Data::Dumper;
use strict;

my @time = localtime();
my $yyyy = $time[5] + 1900;
my $mm   = $time[4] + 1;
if ($mm < 10) { $mm = '0' . $mm }

my $pseed_org      = $ReleaseConfig::pseed_org;
my $coreseed_org   = $ReleaseConfig::coreseed_org;
my $coreseed_sub   = $ReleaseConfig::coreseed_sub;
my $releases       = $ReleaseConfig::releases;
my $subsystems     = $ReleaseConfig::subsystems;

(-s $subsystems) || die "you are missing $subsystems";

my $oldD        = "$releases/.old";
my $newD        = "$releases/.new";
my $problemSetD = "ProblemSets.$yyyy.$mm";
my $destD       = "$releases/$problemSetD";
if (-d $newD)
{
    &SeedUtils::run("rm -rf $newD");
}
mkdir($newD,0777);
mkdir("$newD/GenomeData",0777)     || die "could not make $newD/GenomeData";
mkdir("$newD/SubsystemsData",0777) || die "could not make $newD/SubsystemsData";

my $cs_funcs = {};
open(ALL_PEGS, "| gzip > $newD/all.faa.gz");
my($genome_names,$all_pegs) = &load_genomes("$newD/GenomeData",$coreseed_org,$pseed_org,$cs_funcs, \*ALL_PEGS);
close(ALL_PEGS);
&load_subsystems("$newD/SubsystemsData",$subsystems,$coreseed_sub,$cs_funcs,$genome_names,$coreseed_org,$all_pegs);

if (-d $destD)
{
    &SeedUtils::run("mv $destD $oldD");
}
&SeedUtils::run("mv $newD $destD");
&SeedUtils::run("pushd $releases; tar czf current_release.tgz notes* ProblemSets.$yyyy.$mm");
unlink("$releases/ProblemSets.current");
symlink($problemSetD, "$releases/ProblemSets.current");
if (-d $oldD)
{
    &SeedUtils::run("rm -rf $oldD");
}
#&SeedUtils::run("pushd /homes/overbeek/Ross/AnnotationDataSite/Releases; tar czf current_release.tgz notes* ProblemSets.$yyyy.$mm");


sub load_genomes {
    my($genomeD,$corseseed_org,$pseed_org,$cs_funcs, $unified_output_fh) = @_;

    $ENV{'SAS_SERVER'} = 'PSEED';
    my @tmp = grep { $_->[0] !~ /phage|plasmid|virus/i } map { chomp; [split(/\t/,$_)] } `svr_all_genomes -complete -prokaryotic`;
#   $#tmp = 100;
    my @genomes = map { $_->[1] } sort { $a <=> $b } @tmp;
    my %genome_names = map { ($_->[1] => $_->[0]) } @tmp;

    my $all_pegs = {};
    foreach my $g (@genomes)
    {
    my $genD = "$genomeD/$g";
    mkdir($genD,0777) || die "could not make $genomeD/$g";
    my $type = (-d "$coreseed_org/$g") ? 'c' : 'p';
    open(TYPE,">$genD/type") || die "could not open $genD/type";
    print TYPE "$type\n";
    close(TYPE);
    &SeedUtils::run("cp $pseed_org/$g/contigs $genD/contigs");
    if ($type eq 'c')
    {
#	    print STDERR "$g is a coreSEED genome\n";
        &copy_peg_data($genD,"$coreseed_org/$g",$cs_funcs,$all_pegs, $unified_output_fh);
    }
    else
    {
        &copy_peg_data($genD,"$pseed_org/$g",undef,$all_pegs, $unified_output_fh);
    }
    }
    return (\%genome_names,$all_pegs);
}

sub copy_peg_data {
    my($toD,$fromD,$cs_funcs,$all_pegs, $unified_output_fh) = @_;

    my $gnf;
    my $gname;
    if (open($gnf, "<", "$fromD/GENOME"))
    {
    $gname = <$gnf>;
    chomp $gname;
    close($gnf);
    }

    my %function_of;
    foreach $_ (`cat $fromD/assigned_functions`)
    {
    if ($_ =~ /^(fig\|\d+\.\d+\.peg\.\d+)\t(\S[^\t]*\S)/)
    {
        $function_of{$1} = $2;
    }
    }

    my %loc_of;
    foreach $_ (`cat $fromD/Features/peg/tbl`)
    {
    if ($_ =~ /^(fig\|\d+\.\d+\.peg\.\d+)\t(\S[^\t]*\S)/)
    {
        my $peg = $1;
        my $locs = $2;
        my @locs = split(/,/,$locs);
        my @new_locs = map { &to_new($_) } @locs;
        $loc_of{$peg} = join(",",@new_locs);
    }
    }

    my %tran_of;
    my @tuples = &gjoseqlib::read_fasta("$fromD/Features/peg/fasta");
    foreach $_ (@tuples)
    {
    my($peg,undef,$seq) = @$_;
    $tran_of{$peg} = $seq;
    }
    open(INFO,">$toD/peg-info")   || die "could not open $toD/peg-info";
    open(TRANS,">$toD/peg-trans") || die "could not open $toD/peg-trans";
    foreach my $peg (sort {  &SeedUtils::by_fig_id($a, $b) } keys(%loc_of))
    {
    my $f = $function_of{$peg};
    if (! $f) { $f = "hypothetical protein" }
    if ($cs_funcs) { $cs_funcs->{$peg} = $f }
    my $loc = $loc_of{$peg};
    my $tran = $tran_of{$peg};
    if ($loc && $tran)
    {
        $all_pegs->{$peg} = 1;
        print INFO join("\t",($peg,$loc,$f)),"\n";
        my $info = "$f [$gname]";
        gjoseqlib::print_alignment_as_fasta(\*TRANS, [$peg, $info, $tran]);
        gjoseqlib::print_alignment_as_fasta($unified_output_fh, [$peg, $info, $tran]);
        # print TRANS ">$peg\n$tran\n";
    }
    }
    close(INFO);
    close(TRANS);
}

sub to_new {
    my($old) = @_;

    if ($old =~ /^(\S+)_(\d+)_(\d+)$/)
    {
    my $contig = $1;
    my $from = $2;
    my $to = $3;
    if ($from <= $to)
    {
        my $len = ($to - $from) + 1;
        return $contig . "_" . $from . "+" . $len;
    }
    else
    {
        my $len = ($from - $to) + 1;
        return $contig . "_" . $from . "-" . $len;
    }
    }
    return undef;
}

sub load_subsystems {
    my($subsysD,$subsystems,$seed_subsys,$cs_funcs,$genome_names,$coreseed_org,$all_pegs) = @_;

    my @subsystems =   map { chomp; $_ =~ s/ /_/g; $_ } `cat $subsystems`;
    foreach my $ss (map { chomp; $_ } @subsystems)
    {
    $ss =~ s/ /_/g;
    if (-d "$seed_subsys/$ss")
    {
#	    print STDERR "processing $ss\n";
        my $new_ssD = "$subsysD/$ss";
        mkdir($new_ssD,0777) || die "could not make $new_ssD";
        my @subsys = `cat \'$seed_subsys/$ss/spreadsheet\'`;
#	    print STDERR "got spreadsheet\n";
        open(ROLES,">$new_ssD/Roles") || die "could not open $new_ssD/Roles";
            open(GENOMES,">$new_ssD/GenomesInSubsys") || die "could not open $new_ssD/GenomesInSubsys";
        open(PEGS,">$new_ssD/PegsInSubsys") || die "could not open $new_ssD/PegsInSubsys";

            my $x;
        while (($x = shift @subsys) && ($x !~ /^\/\//))
        {
        if ($x =~ /^\S+\t(\S.*\S)/)
        {
            print ROLES $1 . "\n";
        }
        }
        close(ROLES);
#	    print STDERR "got roles\n";
        while (($x = shift @subsys) && ($x !~ /^\/\//)) {}
        my %pegH;
        while (($x = shift @subsys) && ($x !~ /^\/\//))
        {
        chomp $x;
        my($g,$v,@pegNs)  = split(/\t/,$x);
        next if (($v =~ /^\*/) || (! $genome_names->{$g}));
        next if (! -d "$coreseed_org/$g");             ### take subsystems only from coreSEED
        print GENOMES join("\t",($g,$genome_names->{$g},$v)),"\n";
        my @tmp = map { ($_ =~ /^(\S+)/) ? split(/,/,$_) : () } @pegNs;
        foreach $_ (@tmp)
        {
            my $peg = "fig|$g.peg.$_";
            if ($all_pegs->{$peg})
            {
            my $f   = $cs_funcs->{$peg};
            $pegH{$peg} = $f;
            }
            else
            {
            print STDERR "Subsystem $ss contains invalid peg $peg\n";
            }
        }
        }
        close(GENOMES);
        foreach my $peg (sort { $pegH{$a} cmp $pegH{$b} } keys(%pegH))
        {
        print PEGS join("\t",($peg,$pegH{$peg})),"\n";
        }
        close(PEGS);
    }
    else
    {
        print STDERR "could not find subsystem $seed_subsys/$ss\n";
    }
    }
}

