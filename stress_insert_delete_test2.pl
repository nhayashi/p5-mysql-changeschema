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
while (1) {
    $dbh->do("delete from test2 where id=25000001") or croak $dbh->errstr;
    $dbh->do("insert into test2 values (25000001, 'bbb')") or croak $dbh->errstr;
}
