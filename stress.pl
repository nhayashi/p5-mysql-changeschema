#!/usr/bin/perl
$| = 1;
use strict;
use warnings;

use DBI;
use Parallel::ForkManager;

my $SEL_KEY = 0;
my $INS_KEY = 1;
my $DEL_KEY = 2;
my $UPD_KEY = 3;

my $db   = "test";
my $host = "localhost";
my $user = "root";
my $pass = "root";
my $dsn  = "dbi:mysql:$db:$host";
my %attr = ( RaiseError => 1, AutoCommit => 0 );

my $pm = new Parallel::ForkManager(50);
while (1) {
    $pm->start and next;

    my $dbh = DBI->connect( $dsn, $user, $pass, \%attr );
    my $act_key = int( rand(4) ) % 4;
    if ( $act_key == $SEL_KEY ) {
        my $rows = select_by_id( $dbh, 25000500 );
        if ( $rows->{id} != 25000500 || $rows->{hoge} ne "aaa" ) {
            print "ERROR select\n";
        } else {
            print "SUCCESS select\n";
        }
    }
    elsif ( $act_key == $INS_KEY ) {
        my $last_id = select_last_id($dbh);
        my $ins_rows = insert_rows( $dbh, $last_id + 1, "bbb" );
        if ( $ins_rows != 1 ) {
            print "ERROR insert\n";
            $dbh->rollback;
        }
        else {
            print "SUCCESS insert\n";
            $dbh->commit;
        }
    }
    elsif ( $act_key == $DEL_KEY ) {
        my $last_id = select_last_id($dbh);
        my $del_rows = delete_by_id( $dbh, $last_id );
        if ( $del_rows != 1 ) {
            print "ERROR delete\n";
            $dbh->rollback;
        }
        else {
            print "SUCCESS delete\n";
            $dbh->commit;
        }
    }
    elsif ( $act_key == $UPD_KEY ) {
        my $last_id = select_last_id($dbh);
        my $upd_rows = update_rows( $dbh, $last_id, "ccc" );
        if ( $upd_rows != 1 ) {
            print "ERROR update\n";
            $dbh->rollback;
        }
        else {
            print "SUCCESS update\n";
            $dbh->commit;
        }
    }
    else {
        print "never ocur this statement!\n";
    }

    $dbh->disconnect;

    $pm->finish;
}
$pm->wait_all_children;
print "Finish!\n";

sub select_by_id {
    my ( $dbh, $id ) = @_;
    my $query = "select * from test2 where id=?";
    return $dbh->selectrow_hashref( $query, undef, $id );
}

sub select_last_id {
    my $dbh   = shift;
    my $query = "select id from test2 order by id desc limit 1";
    return ( $dbh->selectrow_array($query) )[0];
}

sub insert_rows {
    my ( $dbh, $id, $val ) = @_;
    my $query = "insert into test2 values (?, ?)";
    return $dbh->do( $query, undef, $id, $val );
}

sub delete_by_id {
    my ( $dbh, $id ) = @_;
    my $query = "delete from test2 where id=?";
    return $dbh->do( $query, undef, $id );
}

sub update_rows {
    my ( $dbh, $id, $val ) = @_;
    my $query = "update test2 set hoge=? where id=?";
    return $dbh->do( $query, undef, $val, $id );
}

