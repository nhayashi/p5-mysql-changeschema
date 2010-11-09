#!/usr/bin/perl
$| = 1;
use strict;
use warnings;

use Carp;
use DBI;

my $db   = "test";
my $user = "root";
my $pass = "root";
my $dsn  = "dbi:mysql:$db:localhost";
my %attr = ( RaiseError => 1, AutoCommit => 1 );
my $dbh  = DBI->connect( $dsn, $user, $pass, \%attr );
$dbh->do("create table test2 (id int unsigned not null, hoge varchar(256), primary key (id)) engine=InnoDB");
for (my $i = 1; $i <= 25_000_000 / 500; $i++) {
    my @vals;
    for my $j (1 .. 500) {
        my $a = $i * 500 + $j;
        print $a."\n";
        push @vals, "(" . $a . ", 'aaa')";
    }
    my $vals = join(",", @vals);
    $dbh->do("insert into test2 values $vals");
}
