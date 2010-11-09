package MySQL::ChangeSchema::Index;

use strict;
use warnings;

use Carp;

sub new {
    my $class = shift;
    scalar @_ < 4
      and croak "should args (table, index, non_unique, is_auto_increment)";
    my %args = @_;
    my $self = bless {
        table             => $args{table},
        name              => $args{name},
        non_unique        => $args{non_unique},
        is_auto_increment => $args{is_auto_increment},
      },
      $class;
}

sub add_column {
    my $self   = shift;
    my $column = shift;
    push @{ $self->{columns} }, $column;
}

sub is_primary {
    my $self = shift;
    $self->{name} eq 'PRIMARY';
}

sub get_drop_sql {
    my $self = shift;
    sprintf "drop index %s on %s", $self->{name}, $self->{table};
}

sub get_create_sql {
    my $self    = shift;
    my @columns = ();
    for my $column ( @{ $self->{columns} } ) {
        my $prefix =
          $column->{prefix} ? sprintf( "(%d)", $column->{prefix} ) : "";
        push @columns, $column->{name} . $prefix;
    }
    my $columns = join( ",", @columns );
    my $unique = $self->{non_unique} ? "" : "unique";
    sprintf( " add %s index %s (%s)", $unique, $self->{name}, $columns );
}

1;
