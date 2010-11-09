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
    $dbh->do("delete from page where page_namespace=0 and page_title='aaa'")
      or croak $dbh->errstr;
    $dbh->do("insert into page (page_namespace, page_title) values (0, 'aaa')")
      or croak $dbh->errstr;
}
