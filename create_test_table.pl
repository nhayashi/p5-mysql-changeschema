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
$dbh->do("drop table if exists test2");
$dbh->do("create table test2 (id int unsigned not null auto_increment, hoge varchar(256), primary key (id)) engine=InnoDB");
for my $i (1 .. 1_000_000 / 500) {
    my @vals;
    for my $j (1 .. 500) {
        push @vals, "('aaa')";
    }
    my $vals = join(",", @vals);
    $dbh->do("insert into test2 (hoge) values $vals");
}

