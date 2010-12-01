#!/usr/bin/perl
$| = 1;
use strict;
use warnings;

use Carp;
use Time::HiRes;

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

my $sel_cnt = 0;
my $ins_cnt = 0;
my $del_cnt = 0;
my $upd_cnt = 0;

my $pm = new Parallel::ForkManager(50);
$pm->run_on_finish(
    sub {
        my $retrieval = $_[5];
            $$retrieval == $SEL_KEY ? $sel_cnt++
          : $$retrieval == $INS_KEY ? $ins_cnt++
          : $$retrieval == $DEL_KEY ? $del_cnt++
          : $$retrieval == $UPD_KEY ? $upd_cnt++
          :                           croak "invalid act_key($$retrieval)";
        print "sel: $sel_cnt ins: $ins_cnt del: $del_cnt upd: $upd_cnt\r";
    }
);
while (1) {
    $pm->start and next;

#    Time::HiRes::sleep(0.1);

    my $dbh = DBI->connect( $dsn, $user, $pass, \%attr );
    my $act_key = int( rand(4) ) % 4;
    if ( $act_key == $SEL_KEY ) {
        $sel_cnt++;
        my $rows = select_by_id( $dbh, 500000 );
        if ( $rows->{id} != 500000 || $rows->{hoge} ne "aaa" ) {
            $act_key = 99;
            print "ERROR select\n";
        }
        else {
#            print "SUCCESS select\n";
        }
    }
    elsif ( $act_key == $INS_KEY ) {
        $ins_cnt++;
        my $ins_rows = insert_rows( $dbh, "bbb" );
        if ( $ins_rows != 1 ) {
            $act_key = 98;
            print "ERROR insert\n";
            $dbh->rollback;
        }
        else {
#            print "SUCCESS insert\n";
            $dbh->commit;
        }
    }
    elsif ( $act_key == $DEL_KEY ) {
        $del_cnt++;
        my $last_id = select_last_id($dbh);
        my $del_rows = delete_by_id( $dbh, $last_id );
        if ( $del_rows != 1 ) {
            $act_key = 97;
#            print "ERROR delete\n";
            $dbh->rollback;
        }
        else {
#            print "SUCCESS delete\n";
            $dbh->commit;
        }
    }
    elsif ( $act_key == $UPD_KEY ) {
        $upd_cnt++;
        my $last_id = select_last_id($dbh);
        my $upd_rows = update_rows( $dbh, $last_id, "ccc" );
        if ( $upd_rows != 1 ) {
            $act_key = 96;
#            print "ERROR update\n";
            $dbh->rollback;
        }
        else {
#            print "SUCCESS update\n";
            $dbh->commit;
        }
    }
    else {
        $act_key = 95;
        print "never ocur this statement!\n";
    }

    $dbh->disconnect;

    $pm->finish( 0, \$act_key );
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
    my ( $dbh, $val ) = @_;
    my $query = "insert into test2 (hoge) values (?)";
    return $dbh->do( $query, undef, $val );
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

