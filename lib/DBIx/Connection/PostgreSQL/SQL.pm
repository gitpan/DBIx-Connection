package DBIx::Connection::PostgreSQL::SQL;

use strict;
use warnings;
use vars qw($VERSION);

use Abstract::Meta::Class ':all';

$VERSION = 0.01;

=head1 NAME

DBIx::Connection::PostgreSQL::SQL - PostgreSQL catalog sql abstractaction layer.

=cut  

=head1 SYNOPSIS
    
    use DBIx::Connection::PostgreSQL::SQL;


=head1 DESCRIPTION

    Represents sql abstractaction layer

=head1 EXPORT

None

=head2 METHODS

=over

=item sequence_value

Returns sql statement that returns next sequence value

=cut

sub sequence_value {
    my ($class, $sequence_name) = @_;
    "SELECT nextval('${sequence_name}') AS val"
}


=item reset_sequence

Returns sql statement that restarts sequence.

=cut

sub reset_sequence {
    my ($class, $sequence_name, $restart_with, $increment_by) = @_;
    ("ALTER SEQUENCE ${sequence_name} RESTART WITH ${restart_with}",
     ($increment_by ? "ALTER SEQUENCE ${sequence_name} INCREMENT  ${increment_by}" : ()));
}


=item has_sequence

Returns sql statement that check is sequence exists in database schema

=cut

sub has_sequence {
    my ($class, $schema) = @_;
    "SELECT pc.relname AS sequence_name
    FROM pg_class pc 
    JOIN pg_authid pa ON pa.oid = pc.relowner AND pc.relkind = 'S' AND  pc.relname = lower(?)  AND rolname = '". $schema ."' ";
}


1;    

__END__

=back

=head1 SEE ALSO

L<DBIx::Connection>

=head1 COPYRIGHT AND LICENSE

The DBIx::Connection::PostgreSQL::SQL module is free software. You may distribute under the terms of
either the GNU General Public License or the Artistic License, as specified in
the Perl README file.

=head1 AUTHOR

Adrian Witas, adrian@webapp.strefa.pl

=cut

1;
