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
use Data::Dumper;
my $rv;
$rv = $dbh->selectall_arrayref("select * from test2 where id=25000000") or croak $dbh->errstr;
print Dumper($rv);
$rv = $dbh->selectall_arrayref("select * from test2 where id=25000001") or croak $dbh->errstr;
print Dumper($rv);
$rv = $dbh->selectall_arrayref("select * from test2 where id=25000002") or croak $dbh->errstr;
print Dumper($rv);
