#!/usr/bin/perl
$| = 1;
use strict;
use warnings;

use Carp;
use DBI;

my $db   = "test";
my $user = "root";
my $pass = "mainuser";
my $dsn  = "dbi:mysql:$db:localhost";
my %attr = ( RaiseError => 1, AutoCommit => 1 );
my $dbh  = DBI->connect( $dsn, $user, $pass, \%attr );
$dbh->do("create table test2 (id int unsigned not null, hoge varchar(256), primary key (id))");
for my $i (1 .. 100_000_000) {
    $dbh->do("insert into test2 values ($i, 'aaa')");
}
