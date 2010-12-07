package MySQL::ChangeSchema;
use strict;
use warnings;
our $VERSION = '0.01';

use Carp;
use DBI;

use MySQL::ChangeSchema::Index;
use MySQL::ChangeSchema::Index::Column;

our $PREFIX = "__osc_";
our $LOCK   = "osc_lock";

our $IDCOLNAME  = "_osc_ID_";
our $DMLCOLNAME = "_osc_dml_type_";

our $DMLTYPE_INSERT = 1;
our $DMLTYPE_DELETE = 2;
our $DMLTYPE_UPDATE = 3;

our $TEMP_TABLE_IDS_TO_EXCLUDE = $PREFIX . "temp_ids_to_exclude";
our $TEMP_TABLE_IDS_TO_INCLUDE = $PREFIX . "temp_ids_to_include";

our $LOCK_MAX_ATTEMPTS = 3;

our $OUTFILE_DIR = undef;
our $LOG_DIR     = "/var/tmp/";

our $VERBOSE = 0;

our $OUTFILE_SIZE = 100000;
our $COMMIT_SIZE  = 500;
our $LONG_TX_TIME = 30;

my %tables = (
    delta_table  => $PREFIX . "chg_",
    new_table    => $PREFIX . "new_",
    rename_table => $PREFIX . "old_",
);

my %triggers = (
    insert_trigger => $PREFIX . "ins_",
    delete_trigger => $PREFIX . "del_",
    update_trigger => $PREFIX . "upd_",
);

my %outfiles = (
    outfile_table   => $PREFIX . "tbl_",
    outfile_exclude => $PREFIX . "ex_",
    outfile_include => $PREFIX . "in_",
);

my %logfiles = (
    log_file   => ".log",
    warn_file  => ".wrn",
    error_file => ".err",
);

use base qw/ Class::Accessor::Fast /;

__PACKAGE__->mk_accessors(
    qw/ db table user pass dbh ddl version reindex /,
    keys %tables,
    keys %triggers,
    keys %outfiles,
    keys %logfiles,
);

sub new {
    my $class = shift;
    my %args  = @_;
    my $self  = bless {}, $class;
    $self->db( $args{db}       or croak "should args db name" );
    $self->table( $args{table} or croak "should args table name" );
    $self->user( $args{user}   or croak "should args user name" );
    $self->pass( $args{pass} || "" );
    map { $self->$_( $tables{$_} . $self->table ) } keys %tables;
    map { $self->$_( $triggers{$_} . $self->table ) } keys %triggers;
    $self;
}

sub connect {
    my $self = shift;
    my $dsn  = sprintf "dbi:mysql:%s:localhost", $self->db;
    my %attr = ( RaiseError => 1, AutoCommit => 0 );
    $self->dbh( DBI->connect( $dsn, $self->user, $self->pass, \%attr ) );
}

sub DESTROY {
    my $self = shift;
    $self->dbh and $self->dbh->disconnect;
}

sub init {
    my $self = shift;

    $OUTFILE_DIR
      or $OUTFILE_DIR =
      ( $self->dbh->selectrow_array('select @@secure_file_priv') )[0];
    $OUTFILE_DIR
      or $OUTFILE_DIR =
      ( $self->dbh->selectrow_array('select @@datadir') )[0] . $self->db . '/';
    map { $self->$_( $OUTFILE_DIR . $outfiles{$_} . $self->table ) }
      keys %outfiles;

    map {
        $self->$_( $LOG_DIR . $PREFIX . "log_" . $self->table . $logfiles{$_} )
    } keys %logfiles;
}

sub cleanup {
    my $self = shift;
    $self->dbh->do( _verbose( 1, 'cleanup', "SET sql_log_bin = 0" ) );
    map { $self->_cleanup( $_->{db}, $_->{obj} ) } @{ $self->get_clean_tables };

    unlink $self->outfile_exclude, $self->outfile_include;
    unlink glob sprintf "%s.*", $self->outfile_table;

    unlink $self->log_file, $self->warn_file, $self->error_file;
}

sub _cleanup {
    my $self  = shift;
    my $db    = shift;
    my $table = shift;
    my $osc   = MySQL::ChangeSchema->new(
        db    => $db,
        table => $table,
        user  => $self->user,
        pass  => $self->pass,
    );
    $osc->connect;
    $osc->get_lock;
    $osc->dbh->do( _verbose( 1, '_cleanup', "unlock tables" ) );
    $osc->dbh->do( _verbose( 1, '_cleanup', "rollback" ) );
    $osc->dbh->do( _verbose( 1, '_cleanup', "set session autocommit=1" ) );

    $osc->drop_trigger( $osc->insert_trigger );
    $osc->drop_trigger( $osc->delete_trigger );
    $osc->drop_trigger( $osc->update_trigger );

    $osc->drop_table( $osc->delta_table );
    if ( $osc->exists_table( $osc->rename_table ) ) {
        ( $osc->exists_table( $osc->table ) )
          ? $osc->drop_table( $osc->rename_table )
          : $osc->alter_rename_table( $osc->rename_table, $osc->table );
    }
    $osc->drop_table( $osc->new_table );

    $osc->dbh->do( _verbose( 1, '_cleanup', "start slave" ) )
      if ( $self->get_slave_status && !$self->{is_slave_running} );
    $osc->release_lock;
    $osc->dbh->disconnect;
}

sub execute {
    my $self = shift;
    my $ddl  = shift;
    $self->ddl( $self->modify_ddl($ddl) );
    $self->check_long_tx;

    $self->validate_version;
    $self->validate_FK;
    $self->validate_trigger;

    # parse column and pk column
    $self->parse_column;
    $self->parse_pk_column;
    $self->decide_reindex;
    $self->dbh->do( _verbose( 1, 'execute', "SET sql_log_bin = 0" ) );
    $self->create_copy_table;
    $self->alter_copy_table;
    $self->parse_new_index;
    $self->validate_post_alter_PK;    # TODO: not implement
    $self->create_delta_table;

    # create triggers
    $self->dbh->do( _verbose( 1, 'execute', "stop slave" ) );
    $self->dbh->do( _verbose( 1, 'execute', "set session autocommit=0" ) );

    #if ( $self->version ne '5.0.84' ) {
    #    $self->lock_tables;
    #}
    $self->create_insert_trigger;
    $self->create_delete_trigger;
    $self->create_update_trigger;

    #if ( $self->version eq '5.0.84' ) {
    #    $self->lock_tables;
    #}
    _verbose(1, 'execute', "commit");
    $self->dbh->commit;

    #$self->dbh->do("unlock tables");
    $self->dbh->do( _verbose( 1, 'execute', "set session autocommit=1" ) );
    $self->dbh->do( _verbose( 1, 'execute', "start slave" ) )
      if ( $self->get_slave_status && !$self->{is_slave_running} );

    # start snapshot tx
    $self->dbh->do(
        _verbose( 1, 'execute', "start transaction with consistent snapshot" )
    );
    $self->store_outfile_exclude;
    $self->create_temporary_table_exclude;
    $self->load_data_exclude;
    unlink $self->outfile_exclude;

    # store outfile table
    $self->store_outfile_table;

    # drop indexes
    $self->drop_indexes if ( $self->reindex );

    # load outfile table
    $self->load_outfile_table;
    $self->replay_changes( temp_table => $TEMP_TABLE_IDS_TO_EXCLUDE );

    # replay changes
    $self->store_outfile_include;
    $self->create_temporary_table_include;
    $self->load_data_include;
    unlink $self->outfile_include;
    $self->replay_changes( temp_table => $TEMP_TABLE_IDS_TO_INCLUDE );
    $self->append_to_excluded_ids;
    $self->drop_temporary_table_include;

    # recreate indexes
    $self->recreate_indexes if ( $self->reindex );

    # replay changes
    $self->store_outfile_include;
    $self->create_temporary_table_include;
    $self->load_data_include;
    unlink $self->outfile_include;
    $self->replay_changes( temp_table => $TEMP_TABLE_IDS_TO_INCLUDE );
    $self->append_to_excluded_ids;
    $self->drop_temporary_table_include;

    # swap table
    $self->dbh->do( _verbose( 1, 'execute', "stop slave" ) );
    $self->dbh->do( _verbose( 1, 'execute', "set session autocommit=0" ) );
    $self->lock_tables( both => 1 );
    $self->store_outfile_include;
    $self->create_temporary_table_include;
    $self->load_data_include;
    unlink $self->outfile_include;
    $self->replay_changes(
        temp_table => $TEMP_TABLE_IDS_TO_INCLUDE,
        use_tx     => 1
    );
    $self->append_to_excluded_ids;
    $self->drop_temporary_table_include;
    $self->checksum if (0);    # TODO: don't support checksum option
    $self->rename_tables( $self->table, $self->rename_table, $self->new_table );
    _verbose(1, 'execute', "commit");
    $self->dbh->commit;
    $self->dbh->do( _verbose( 1, 'execute', "unlock tables" ) );
    $self->dbh->do( _verbose( 1, 'execute', "set session autocommit=1" ) );
    $self->dbh->do( _verbose( 1, 'execute', "start slave" ) )
      if ( $self->get_slave_status && !$self->{is_slave_running} );

    # cleanup
    $self->cleanup;
}

# ----

sub get_slave_status {
    my $self   = shift;
    my $status = $self->dbh->selectrow_hashref(_verbose(1, 'get_slave_status', "show slave status"));
    return unless ($status);    # not use slave
    if (    $status->{Slave_IO_Running} eq 'Yes'
        and $status->{Slave_SQL_Running} eq 'Yes' )
    {
        $self->{is_slave_running} = 1;
    }
    else {
        $self->{is_slave_running} = 0;
    }
    1;
}

sub rename_tables {
    my $self = shift;
    my ( $old, $tmp, $new ) = @_;
    my $query = "rename table %s to %s, %s to %s";
    my $combined_query = sprintf $query, $old, $tmp, $new, $old;
    _verbose( 1, 'rename_tables', $combined_query );
    $self->dbh->do($combined_query);
}

sub drop_table {
    my $self           = shift;
    my $table          = shift or croak "should args table name";
    my $query          = "drop table if exists %s";
    my $combined_query = sprintf $query, $table;
    _verbose( 1, 'drop_table', $combined_query );
    $self->dbh->do($combined_query);
}

sub drop_trigger {
    my $self           = shift;
    my $trigger        = shift or croak "should args trigger name";
    my $query          = "drop trigger if exists %s";
    my $combined_query = sprintf $query, $trigger;
    _verbose( 1, 'drop_trigger', $combined_query );
    $self->dbh->do($combined_query);
}

sub exists_table {
    my $self           = shift;
    my $table          = shift or croak "should args table name";
    my $query          = "show tables like '%s'";
    my $combined_query = sprintf $query, $table;
    _verbose( 1, 'exists_table', $combined_query );
    $self->dbh->do($combined_query);
}

sub exists_trigger {
    my $self    = shift;
    my $trigger = shift or croak "should args trigger name";
    my $query   = <<"SQL";
    select count(*) from information_schema.triggers T
     where T.trigger_schema like %s and T.trigger_name like %s
SQL
    my $combined_query = sprintf $query, $self->db, $trigger;
    _verbose( 1, 'exists_trigger', $combined_query );
    ( $self->dbh->selectrow_array($combined_query) )[0];
}

sub get_clean_tables {
    my $self           = shift;
    my $combined_query = <<"SQL";
    (select T.table_schema as db, substr(T.table_name, 11) as obj
       from information_schema.tables T
      where T.table_name like '$PREFIX%')
      union distinct
    (select T.trigger_schema as db, substr(T.trigger_name, 11) as obj
       from information_schema.triggers T
      where T.trigger_name like '$PREFIX%')
      order by db, obj
SQL
    _verbose( 1, 'get_clean_tables', $combined_query );
    $self->dbh->selectall_arrayref( $combined_query, { Slice => {} } );
}

sub get_lock {
    my $self           = shift;
    my $query          = "select get_lock('%s', 0)";
    my $combined_query = sprintf $query, $LOCK;
    _verbose( 1, 'get_lock', $combined_query );
    ( $self->dbh->selectrow_array($combined_query) )[0] == 1
      or croak "couldn't get lock $LOCK";
}

sub release_lock {
    my $self           = shift;
    my $query          = "do release_lock('%s')";
    my $combined_query = sprintf $query, $LOCK;
    _verbose( 1, 'release_lock', $combined_query );
    $self->dbh->do($combined_query);
}

sub lock_tables {
    my $self        = shift;
    my %args        = @_;
    my $query       = "lock table %s WRITE";
    my @bind_values = ( $self->table );
    if ( $args{both} ) {
        $query .= ", %s WRITE";
        push @bind_values, $self->new_table;
    }
    my $i              = 0;
    my $combined_query = sprintf $query, @bind_values;
    my $sth            = $self->dbh->prepare($combined_query);
    while ( ++$i <= $LOCK_MAX_ATTEMPTS ) {
        _verbose( 1, 'lock_tables', $combined_query );
        $sth->execute;
        $sth->err or return;

        # 1205 is timeout and 1213 is deadlock
        next if ( $sth->err == 1205 || $sth->err == 1213 );

        croak "couldn't lock table(s). " . $sth->errstr;
    }
    croak "couldn't lock table(s). " . $sth->errstr;
}

sub modify_ddl {
    my $self      = shift;
    my $ddl       = shift;
    my $table     = $self->table;
    my $qtableq   = $self->dbh->quote( $self->table );
    my $new_table = $self->new_table;
    my $match =
      $ddl =~ s/ALTER\s+TABLE\s+($table|$qtableq)/ALTER TABLE $new_table/i;
    $match or croak "invalid alter ddl";
    $ddl;
}

sub check_long_tx {
    my $self = shift;
    my @long_proc;
    my $query = "show full processlist";
    _verbose(1, 'check_long_tx', $query);
    my $processes = $self->dbh->selectall_arrayref( $query, { Slice => {} } );
    for my $process ( @{$processes} ) {
        next
          if ( ( !$process->{Time} || $process->{Time} < $LONG_TX_TIME )
            || $process->{db} ne $self->db
            || $process->{Command} eq 'Sleep' );
        push @long_proc,
          sprintf "Id=%d,User=%s,Host=%s,db=%s,Command=%s,Time=%d,Info=%s",
          $process->{qw/Id User Host db Command Time Info/};
    }
    scalar @long_proc > 0
      and croak "long running tx(s) found.\n" . join( "\n", @long_proc );
}

sub validate_version {
    my $self = shift;
    $self->version( $self->get_version )
      unless ( $self->version );
    croak "didn't test this version: " . $self->version
      unless ( $self->version eq '5.0.84'
        || $self->version eq '5.1.47'
        || $self->version eq '5.1.49' );
}

sub get_version {
    my $self    = shift;
    my $version = ( $self->dbh->selectrow_array("select version()") )[0];
    $version =~ /^(\d+\.\d+\.\d+)/;
    $version = $1;
}

sub validate_FK {
    my $self  = shift;
    my $query = <<"SQL";
    select count(*) from information_schema.key_column_usage
     where referenced_table_name is not null and
     ((table_schema=? and table_name=?) or
      (referenced_table_schema=? and referenced_table_name=?))
SQL
    my @bind_values = ( $self->db, $self->table, $self->db, $self->table );
    _verbose( 1, 'validate_FK', $query, @bind_values );
    ( $self->dbh->selectrow_array( $query, undef, @bind_values ) )[0]
      and croak "don't support the table have FK";
}

sub validate_trigger {
    my $self  = shift;
    my $query = <<"SQL";
    select count(*) from information_schema.triggers
     where event_object_schema=? and event_object_table=?
SQL
    my @bind_values = ( $self->db, $self->table );
    _verbose( 1, 'validate_trigger', $query, @bind_values );
    ( $self->dbh->selectrow_array( $query, undef, @bind_values ) )[0]
      and croak "don't support the table have trigger";
}

sub parse_column {
    my $self  = shift;
    my $query = <<"SQL";
    select column_name, column_key, extra
      from information_schema.columns
     where table_schema=? and table_name=?
SQL
    my @bind_values = ( $self->db, $self->table );
    _verbose( 1, 'parse_column', $query, @bind_values );
    my $sth = $self->dbh->prepare($query);
    $sth->execute(@bind_values);
    my @columns;
    my @no_pk_columns;

    while ( my $column = $sth->fetchrow_hashref ) {
        push @columns, $column->{column_name};
        if ( $column->{extra} =~ /auto_increment/ ) {
            $self->{auto_increment} = $column->{column_name};
        }
        if ( $column->{column_key} ne 'PRI' ) {
            push @no_pk_columns, $column->{column_name};
        }
    }
    if ( @columns > 0 ) {
        $self->{columns_arrayref} = \@columns;
        $self->{columns}          = join( ", ", @columns );
        $self->{old_columns}      = join( ", ", map { "OLD." . $_ } @columns );
        $self->{new_columns}      = join( ", ", map { "NEW." . $_ } @columns );
    }
    if ( @no_pk_columns > 0 ) {
        $self->{no_pk_columns_arrayref} = \@no_pk_columns;
        $self->{no_pk_columns} = join( ", ", @no_pk_columns );
    }
}

sub parse_pk_column {
    my $self  = shift;
    my $query = <<"SQL";
    select * from information_schema.statistics
     where table_schema=? and table_name=? and index_name=?
     order by index_name, seq_in_index
SQL
    my @bind_values = ( $self->db, $self->table, 'PRIMARY' );
    _verbose( 1, 'parse_ok_column', $query, @bind_values );
    my $sth = $self->dbh->prepare($query);
    $sth->execute(@bind_values);
    my @pk_columns;
    while ( my $pk_columns = $sth->fetchrow_hashref ) {
        push @pk_columns, $pk_columns->{COLUMN_NAME};
    }
    if ( @pk_columns > 0 ) {
        $self->{pk_columns_arrayref} = \@pk_columns;
        $self->{pk_columns} = join( ", ", @pk_columns );
        $self->{old_pk_columns} = join( ", ", map { "OLD." . $_ } @pk_columns );
        $self->{new_pk_columns} = join( ", ", map { "NEW." . $_ } @pk_columns );
    }

    # set range variables
    my $count = scalar @pk_columns;
    my @range_start;
    my @range_end;
    for ( my $i = 0 ; $i < $count ; $i++ ) {
        my $range_start = sprintf '@range_start_%d', $i;
        my $range_end   = sprintf '@range_end_%d',   $i;
        push @range_start, $range_start;
        push @range_end,   $range_end;
    }
    $self->{range_start_arrayref} = \@range_start;
    $self->{range_end_arrayref}   = \@range_end;
    $self->{range_start}          = join( ", ", @range_start );
    $self->{range_end}            = join( ", ", @range_end );
}

sub decide_reindex {
    my $self = shift;
    $self->reindex( $self->version eq '5.1.47' || $self->version eq '5.1.49' );
}

sub create_copy_table {
    my $self           = shift;
    my $query          = "create table %s like %s";
    my $combined_query = sprintf $query, $self->new_table, $self->table;
    _verbose( 1, 'create_copy_table', $combined_query );
    $self->dbh->do($combined_query);
}

sub alter_copy_table {
    my $self = shift;
    _verbose( 1, 'alter_copy_table', $self->ddl );
    $self->dbh->do( $self->ddl );
}

sub parse_new_index {
    my $self  = shift;
    my $query = <<"SQL";
    select * from information_schema.statistics
     where table_schema=? and table_name=?
     order by index_name, seq_in_index
SQL
    my @bind_values = ( $self->db, $self->new_table );
    _verbose( 1, 'parse_new_index', $query, @bind_values );
    my $sth = $self->dbh->prepare($query);
    $sth->execute(@bind_values);
    my $prev_index_name = '';
    my $index           = undef;
    my $primary         = undef;
    while ( my $row = $sth->fetchrow_hashref ) {

        if ( $prev_index_name ne $row->{INDEX_NAME} ) {
            my $auto =
              (      $self->{auto_increment}
                  && $self->{auto_increment} eq $row->{COLUMN_NAME} ) ? 1 : 0;
            $index = MySQL::ChangeSchema::Index->new(
                table             => $self->new_table,
                name              => $row->{INDEX_NAME},
                non_unique        => $row->{NON_UNIQUE},
                is_auto_increment => $auto,
            );
            push @{ $self->{indexes} }, $index;
            $primary = $index if ( $index->is_primary );
        }
        my $column = MySQL::ChangeSchema::Index::Column->new(
            name   => $row->{COLUMN_NAME},
            prefix => $row->{SUB_PART},
        );
        push @{ $index->{columns} }, $column;
        $prev_index_name = $row->{INDEX_NAME};
    }
    croak "No primary key defined in the new table!" unless ($primary);
}

sub validate_post_alter_PK {
    my $self = shift;
    for ( my $index = $self->{indexes} ) {

        # TODO: array_slice, array_diff on columns
    }
}

sub create_delta_table {
    my $self  = shift;
    my $query = <<"SQL";
    create table %s (%s int auto_increment, %s int, primary key(%s))
     as (select %s from %s limit 0)
SQL
    my @bind_values = (
        $self->delta_table, $IDCOLNAME,       $DMLCOLNAME,
        $IDCOLNAME,         $self->{columns}, $self->table
    );
    my $combined_query = sprintf $query, @bind_values;
    _verbose( 1, 'create_delta_table', $combined_query );
    $self->dbh->do($combined_query);
}

sub create_insert_trigger {
    my $self  = shift;
    my $query = <<"SQL";
    create trigger %s after insert on %s for each row
    insert into %s(%s, %s) values (%d, %s)
SQL
    my @bind_values = (
        $self->insert_trigger, $self->table,     $self->delta_table,
        $DMLCOLNAME,           $self->{columns}, $DMLTYPE_INSERT,
        $self->{new_columns}
    );
    my $combined_query = sprintf $query, @bind_values;
    _verbose( 1, 'create_insert_trigger', $combined_query );
    $self->dbh->do($combined_query);
}

sub create_delete_trigger {
    my $self  = shift;
    my $query = <<"SQL";
    create trigger %s after delete on %s for each row
    insert into %s(%s, %s) values (%d, %s)
SQL
    my @bind_values = (
        $self->delete_trigger, $self->table,        $self->delta_table,
        $DMLCOLNAME,           $self->{pk_columns}, $DMLTYPE_DELETE,
        $self->{old_pk_columns}
    );
    my $combined_query = sprintf $query, @bind_values;
    _verbose( 1, 'create_delete_trigger', $combined_query );
    $self->dbh->do($combined_query);
}

sub create_update_trigger {
    my $self = shift;
    my $cond =
      join( ' AND ',
        map { "NEW.$_=OLD.$_" } @{ $self->{pk_columns_arrayref} } );
    my $query = <<"SQL";
    create trigger %s after update on %s for each row
    IF (%s) THEN
      insert into %s(%s, %s) values (%d, %s);
    ELSE
      insert into %s(%s, %s) values (%d, %s), (%d, %s);
    END IF
SQL
    my @bind_values = (
        $self->update_trigger, $self->table,         $cond,
        $self->delta_table,    $DMLCOLNAME,          $self->{columns},
        $DMLTYPE_UPDATE,       $self->{new_columns}, $self->delta_table,
        $DMLCOLNAME,           $self->{columns},     $DMLTYPE_DELETE,
        $self->{old_columns},  $DMLTYPE_INSERT,      $self->{new_columns}
    );
    my $combined_query = sprintf $query, @bind_values;
    _verbose( 1, 'create_update_trigger', $combined_query );
    $self->dbh->do($combined_query);
}

sub store_outfile_exclude {
    my $self  = shift;
    my $query = <<"SQL";
    select %s, %s from %s order by %s into outfile '%s'
SQL
    my @bind_values = (
        $IDCOLNAME, $DMLCOLNAME, $self->delta_table, $IDCOLNAME,
        $self->outfile_exclude
    );
    my $combined_query = sprintf $query, @bind_values;
    _verbose( 1, 'store_outfile_exclude', $combined_query );
    $self->dbh->do($combined_query);
}

sub create_temporary_table_exclude {
    my $self  = shift;
    my $query = <<"SQL";
    create temporary table %s(%s int, %s int, primary key (%s))
SQL
    my @bind_values =
      ( $TEMP_TABLE_IDS_TO_EXCLUDE, $IDCOLNAME, $DMLCOLNAME, $IDCOLNAME );
    my $combined_query = sprintf $query, @bind_values;
    _verbose( 1, 'create_temporary_table_exclude', $combined_query );
    $self->dbh->do($combined_query);
}

sub load_data_exclude {
    my $self  = shift;
    my $query = <<"SQL";
    load data infile '%s' into table %s(%s, %s)
SQL
    my @bind_values = (
        $self->outfile_exclude, $TEMP_TABLE_IDS_TO_EXCLUDE, $IDCOLNAME,
        $DMLCOLNAME
    );
    my $combined_query = sprintf $query, @bind_values;
    _verbose( 1, 'load_data_exclude', $combined_query );
    $self->dbh->do($combined_query);
}

sub store_outfile_include {
    my $self          = shift;
    my $delta_idcol   = sprintf "%s.%s", $self->delta_table, $IDCOLNAME;
    my $delta_dmlcol  = sprintf "%s.%s", $self->delta_table, $DMLCOLNAME;
    my $exclude_idcol = sprintf "%s.%s", $TEMP_TABLE_IDS_TO_EXCLUDE, $IDCOLNAME;
    my $query         = <<"SQL";
    select %s, %s from %s left join %s on %s = %s
     where %s is null order by %s into outfile '%s'
SQL
    my @bind_values = (
        $delta_idcol,       $delta_dmlcol,
        $self->delta_table, $TEMP_TABLE_IDS_TO_EXCLUDE,
        $delta_idcol,       $exclude_idcol,
        $exclude_idcol,     $delta_idcol,
        $self->outfile_include
    );
    my $combined_query = sprintf $query, @bind_values;
    _verbose( 1, 'store_outfile_include', $combined_query );
    $self->dbh->do($combined_query);
}

sub create_temporary_table_include {
    my $self  = shift;
    my $query = <<"SQL";
    create temporary table %s(%s int, %s int, primary key (%s))
SQL
    my @bind_values =
      ( $TEMP_TABLE_IDS_TO_INCLUDE, $IDCOLNAME, $DMLCOLNAME, $IDCOLNAME );
    my $combined_query = sprintf $query, @bind_values;
    _verbose( 1, 'create_temporary_table_include', $combined_query );
    $self->dbh->do($combined_query);
}

sub drop_temporary_table_include {
    my $self = shift;
    my $combined_query =
      sprintf( "drop temporary table %s", $TEMP_TABLE_IDS_TO_INCLUDE );
    _verbose( 1, 'drop_temporary_table_include', $combined_query );
    $self->dbh->do($combined_query);
}

sub load_data_include {
    my $self  = shift;
    my $query = <<"SQL";
    load data infile '%s' into table %s(%s, %s)
SQL
    my @bind_values = (
        $self->outfile_include, $TEMP_TABLE_IDS_TO_INCLUDE, $IDCOLNAME,
        $DMLCOLNAME
    );
    my $combined_query = sprintf $query, @bind_values;
    _verbose( 1, 'load_data_include', $combined_query );
    $self->dbh->do($combined_query);
}

sub store_outfile_table {
    my $self = shift;

    my $row_count;
    my $pk_count = scalar @{ $self->{pk_columns_arrayref} };

    my $whereclause    = "";
    my $outfile_suffix = 0;
    do {
        $outfile_suffix++;

        my @assign_range_end = ();
        for ( my $i = 0 ; $i < $pk_count ; $i++ ) {
            my $range_end = $self->{range_end_arrayref}->[$i];
            my $pk_column = $self->{pk_columns_arrayref}->[$i];
            push @assign_range_end,
              sprintf( "%s := %s", $range_end, $pk_column );
        }
        my $assign_range_end = join( ", ", @assign_range_end );

        my $query = <<"SQL";
        select %s, %s from %s %s order by %s limit %d into outfile '%s.%d'
SQL
        my @bind_values = (
            $assign_range_end,    $self->{no_pk_columns},
            $self->table,         $whereclause,
            $self->{pk_columns},  $OUTFILE_SIZE,
            $self->outfile_table, $outfile_suffix
        );
        my $combined_query1 = sprintf $query, @bind_values;
        _verbose( 1, 'store_outfile_table', $combined_query1 );
        $row_count = $self->dbh->do($combined_query1);

        $self->{outfile_suffix_start} = 1;
        $self->{outfile_suffix_end}   = $outfile_suffix;

        my $combined_query2 = sprintf "select %s into %s",
          $self->{range_end}, $self->{range_start};
        _verbose( 1, 'store_outfile_table', $combined_query2 );
        $self->dbh->do($combined_query2);

        if ( $outfile_suffix == 1 ) {
            my $range    = "";
            my @equality = ();
            my @cond     = ();
            for ( my $i = 0 ; $i < $pk_count ; $i++ ) {
                my $pk_column   = $self->{pk_columns_arrayref}->[$i];
                my $range_start = $self->{range_start_arrayref}->[$i];
                $range = sprintf( "%s > %s", $pk_column, $range_start );
                if ( $i > 0 ) {
                    my $prev_pk    = $self->{pk_columns_arrayref}->[ $i - 1 ];
                    my $prev_range = $self->{range_start_arrayref}->[ $i - 1 ];
                    push @equality, sprintf( "%s = %s", $prev_pk, $prev_range );
                }
                my $equality = join( " and ", @equality );
                $equality and $equality .= " and ";
                push @cond, sprintf( "(%s)", $equality . $range );
            }
            $whereclause = sprintf( "where (%s)", join( " or ", @cond ) );
        }
    } while ( $row_count >= $OUTFILE_SIZE );
    _verbose(1, 'store_outfile_table', "commit");
    $self->dbh->commit;
}

sub drop_indexes {
    my $self = shift;
    for my $index ( @{ $self->{indexes} } ) {
        unless ( $index->is_primary || $index->{is_auto_increment} ) {
            my $query = $index->get_drop_sql;
            _verbose( 1, 'drop_indexes', $query );
            $self->dbh->do($query);
        }
    }
}

sub load_outfile_table {
    my $self = shift;
    while ( $self->{outfile_suffix_end} >= $self->{outfile_suffix_start} ) {
        my $query   = "load data infile '%s' into table %s(%s, %s)";
        my $outfile = sprintf( "%s.%d",
            $self->outfile_table, $self->{outfile_suffix_start} );
        my @bind_values = (
            $outfile, $self->new_table, $self->{pk_columns},
            $self->{no_pk_columns}
        );
        my $combined_query = sprintf $query, @bind_values;
        _verbose( 1, 'load_outfile_table', $combined_query );
        $self->dbh->do($combined_query);

        unlink $outfile;
        $self->{outfile_suffix_start}++;
    }
}

sub recreate_indexes {
    my $self    = shift;
    my @creates = ();
    for my $index ( @{ $self->{indexes} } ) {
        unless ( $index->is_primary || $index->{is_auto_increment} ) {
            push @creates, $index->get_create_sql;
        }
    }
    if ( @creates > 0 ) {
        my $query = "alter table " . $self->new_table . join( ", ", @creates );
        _verbose( 1, 'recreate_indexes', $query );
        $self->dbh->do($query);
    }
}

sub replay_changes {
    my $self = shift;
    my %args = @_;

    my @bind_values =
      ( $IDCOLNAME, $DMLCOLNAME, $args{temp_table}, $IDCOLNAME );
    my $query = sprintf "select %s, %s from %s order by %s", @bind_values;
    _verbose( 1, 'replay_changes', $query );
    my $changes = $self->dbh->selectall_arrayref( $query, { Slice => {} } );

    if ( $args{use_tx} ) {
        $self->dbh->do( _verbose( 1, 'replay_changes', "start transaction" ) );
    }

    my $i = 0;
    for my $change ( @{$changes} ) {
        $i++;
        if ( $args{use_tx} && ( $i % $COMMIT_SIZE == 0 ) ) {
            _verbose(1, 'replay_changes', "commit");
            $self->dbh->commit;
        }
        if ( $change->{$DMLCOLNAME} == $DMLTYPE_INSERT ) {
            my $insert =
              "insert into %s(%s) select %s from %s where %s.%s = %d";
            my @bind_params = (
                $self->new_table,   $self->{columns},
                $self->{columns},   $self->delta_table,
                $self->delta_table, $IDCOLNAME,
                $change->{$IDCOLNAME}
            );
            my $combined_query = sprintf $insert, @bind_params;
            _verbose( 2, 'replay_changes', $combined_query );
            my $rv = $self->dbh->do($combined_query);
            $rv != 1
              and croak "[$insert] affected $rv rows instead of 1 row";
        }
        elsif ( $change->{$DMLCOLNAME} == $DMLTYPE_DELETE ) {
            my $delete      = "delete %s from %s, %s where %s.%s = %d and %s";
            my $whereclause = join(
                " and ",
                map {
                    sprintf( "%s.%s = %s.%s",
                        $self->new_table, $_, $self->delta_table, $_ )
                  } @{ $self->{pk_columns_arrayref} }
            );
            my @bind_params = (
                $self->new_table,   $self->new_table,
                $self->delta_table, $self->delta_table,
                $IDCOLNAME,         $change->{$IDCOLNAME},
                $whereclause
            );
            my $combined_query = sprintf $delete, @bind_params;
            _verbose( 2, 'replay_changes', $combined_query );
            my $rv = $self->dbh->do($combined_query);
            $rv != 1
              and croak "[$delete] affected $rv rows instead of 1 row";
        }
        elsif ( $change->{$DMLCOLNAME} == $DMLTYPE_UPDATE ) {
            my $update = "update %s, %s set %s where %s.%s = %d and %s";
            my $assign = join(
                ", ",
                map {
                    sprintf( "%s.%s = %s.%s",
                        $self->new_table, $_, $self->delta_table, $_ )
                  } @{ $self->{no_pk_columns_arrayref} }
            );
            my $whereclause = join(
                " and ",
                map {
                    sprintf( "%s.%s = %s.%s",
                        $self->new_table, $_, $self->delta_table, $_ )
                  } @{ $self->{pk_columns_arrayref} }
            );
            my @bind_params = (
                $self->new_table, $self->delta_table, $assign,
                $self->delta_table, $IDCOLNAME, $change->{$IDCOLNAME},
                $whereclause
            );
            my $combined_query = sprintf $update, @bind_params;
            _verbose( 2, 'replay_changes', $combined_query );
            my $rv = $self->dbh->do($combined_query);
            $rv != 1
              and croak "[$update] affected $rv rows instead of 1 row";
        }
    }
    if ( $args{use_tx} ) {
        _verbose(1, 'replay_changes', "commit");
        $self->dbh->commit;
    }
}

sub append_to_excluded_ids {
    my $self        = shift;
    my $query       = "insert into %s(%s, %s) select %s, %s from %s";
    my @bind_values = (
        $TEMP_TABLE_IDS_TO_EXCLUDE, $IDCOLNAME, $DMLCOLNAME, $IDCOLNAME,
        $DMLCOLNAME, $TEMP_TABLE_IDS_TO_INCLUDE
    );
    my $combined_query = sprintf $query, @bind_values;
    _verbose( 1, 'append_to_exclude_ids', $combined_query );
    $self->dbh->do($combined_query);
}

sub checksum {
    my $self           = shift;
    my $query          = "checksum table %s, %s";
    my @bind_values    = ( $self->new_table, $self->table );
    my $combined_query = sprintf $query, @bind_values;
    _verbose(1, 'checksum', $combined_query);
    my $checksums =
      $self->dbh->selectall_arrayref( $combined_query, { Slice => {} } );
    $checksums->[0]->{Checksum} == $checksums->[1]->{Checksum}
      or croak "checksums don't match";
}

sub _verbose {
    my $level = shift;
    my $func  = shift;
    my $query = shift;
    if ( $VERBOSE >= $level ) {
        my $bind = ( scalar @_ ) ? "\n[" . join( ', ', @_ ) . "]" : "";

        #        print "# " . $func . ": " . $query . $bind . "\n";
        $query =~ s/^[\s\t]+//g;
        $query =~ s/\r?\n//g;
        $query =~ s/\s+/ /g;
        print "# " . $query . $bind . "\n";
    }
    return $query;
}

1;
__END__

=head1 NAME

MySQL::ChangeSchema -

=head1 SYNOPSIS

  use MySQL::ChangeSchema;

=head1 DESCRIPTION

MySQL::ChangeSchema is

=head1 AUTHOR

nhayashi E<lt>naritoshi.hayashi@gmail.comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
