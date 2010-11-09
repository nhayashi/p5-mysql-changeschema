package MySQL::ChangeSchema::Index::Column;

use strict;
use warnings;

use Carp;

sub new {
    my $class = shift;
    scalar @_ < 2
      and croak "should args column name and prefix(SUB_PART)";
    my %args = @_;
    my $self = bless {
        name   => $args{name},
        prefix => $args{prefix},
      },
      $class;
}

1;
