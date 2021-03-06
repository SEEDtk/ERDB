use strict;
use FIG_Config;
use ScriptUtils;
use ProtFamRepo;
use Stats;
use GPUtils;
use P3DataAPI;
use Digest::MD5;
use Encode qw(encode_utf8);
use Shrub;

my $shrub = Shrub->new();
open(my $ih, "<$FIG_Config::p3data/roles.in.subsystems") || die "Could not open input roles.in.subsystems: $!";
open(my $oh, ">$FIG_Config::data/roles.in.subsystems") || die "Could not open output roles.in.subsystems: $!";
open(my $rh, ">$FIG_Config::data/Inputs/Other/roles.tbl") || die "Could not open roles.tbl: $!";
while (! eof $ih) {
    my $line = <$ih>;
    chomp $line;
    my ($id, $checksum, $role) = split /\t/, $line;
    my $subs = ($role =~ s/\r//g);
    if ($subs) {
        print "CRs found in $id: $role\n";
    }
    my $newCheck = RoleParse::Checksum($role);
    if ($newCheck ne $checksum) {
        print "Correcting $id: $role.\n";
    }
    print $oh join("\t", $id, $newCheck, $role) . "\n";
    my ($roleData) = $shrub->GetAll('Role', 'Role(id) = ?', [$id], 'id checksum ec-number tc-number');
    if ($roleData) {
        print $rh join("\t", @$roleData) . "\n";
    } else {
        print "Could not find $id: $role.\n";
    }

}
