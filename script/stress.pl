#!/usr/bin/perl
$| = 1;
use strict;
use warnings;

use Carp;
use Time::HiRes;

use DBI;
use Parallel::ForkManager;

my $db   = "test";
my $host = "localhost";
my $user = "root";
my $pass = "root";
my $dsn  = "dbi:mysql:$db:$host";
my %attr = ( RaiseError => 1, AutoCommit => 0 );

my $max_id = 100_000;

my $pm = new Parallel::ForkManager(100);
$pm->run_on_finish(
    sub {
        my $retrieval = $_[5];
        if ($$retrieval != 0) {
            die "err: $$retrieval\r";
        }
    }
);

my $id = 0;
while (1) {
    $id = ( $id >= $max_id ) ? 1 : $id + 1;

    $pm->start and next;

    my $err = 0;

    # select
    {
        my $dbh = DBI->connect( $dsn, $user, $pass, \%attr );
        my $hoge = (
            $dbh->selectrow_array(
                "select hoge from test2 where id=?",
                undef, $id
            )
        )[0];
        ( $hoge ne 'aaa' ) and $err += 1;
        $dbh->disconnect;
    }

    # update
    {
        my $dbh = DBI->connect( $dsn, $user, $pass, \%attr );
        my $row =
          $dbh->do( "update test2 set hoge='bbb' where id=?", undef, $id );
        if ( $row != 1 ) {
            $err += 2;
            $dbh->rollback;
        }
        else {
            $dbh->commit;
        }
        $dbh->disconnect;
    }

    # select
    {
        my $dbh = DBI->connect( $dsn, $user, $pass, \%attr );
        my $hoge = (
            $dbh->selectrow_array(
                "select hoge from test2 where id=?",
                undef, $id
            )
        )[0];
        ( $hoge ne 'bbb' ) and $err += 4;
        $dbh->disconnect;
    }

    # delete
    {
        my $dbh = DBI->connect( $dsn, $user, $pass, \%attr );
        my $row = $dbh->do( "delete from test2 where id=?", undef, $id );
        if ( $row != 1 ) {
            $err += 8;
            $dbh->rollback;
        }
        else {
            $dbh->commit;
        }
        $dbh->disconnect;
    }

    # select
    {
        my $dbh = DBI->connect( $dsn, $user, $pass, \%attr );
        my $count = (
            $dbh->selectrow_array(
                "select count(*) from test2 where id=?",
                undef, $id
            )
        )[0];
        ( $count != 0 ) and $err += 16;
        $dbh->disconnect;
    }

    # insert
    {
        my $dbh = DBI->connect( $dsn, $user, $pass, \%attr );
        my $row = $dbh->do( "insert into test2 (id, hoge) values (?, 'aaa')",
            undef, $id );
        if ( $row != 1 ) {
            $err += 32;
            $dbh->rollback;
        }
        else {
            $dbh->commit;
        }
        $dbh->disconnect;
    }

    # select
    {
        my $dbh = DBI->connect( $dsn, $user, $pass, \%attr );
        my $hoge = (
            $dbh->selectrow_array(
                "select hoge from test2 where id=?",
                undef, $id
            )
        )[0];
        ( $hoge ne 'aaa' ) and $err += 64;
        $dbh->disconnect;
    }

    $pm->finish( 0, \$err );

}
$pm->wait_all_children;

