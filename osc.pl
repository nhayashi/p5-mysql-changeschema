#!/usr/bin/perl

use strict;
use warnings;

use MySQL::ChangeSchema;

my $db    = "test";
my $table = "test2";
my $user  = "root";
my $pass  = "root";
my $ddl   = "ALTER TABLE $table ADD fuga int";

my $osc = MySQL::ChangeSchema->new(
    db    => $db,
    table => $table,
    user  => $user,
    pass  => $pass
);
$osc->connect();
$osc->init();
$osc->cleanup();
eval { $osc->execute($ddl); };

if ($@) {
    print $@."\n";
    $osc->cleanup;
}

