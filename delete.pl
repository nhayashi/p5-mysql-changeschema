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
my $rv = $dbh->do("delete from page where page_id=76561");
print $rv."\n";

