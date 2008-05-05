package DBIx::Connection::Oracle::SQL;

use strict;
use warnings;
use vars qw($VERSION);

use Abstract::Meta::Class ':all';

$VERSION = 0.01;

=head1 NAME

DBIx::Connection::Oracle::SQL - Oracle catalog sql abstractaction layer.

=cut  

=head1 SYNOPSIS
    
    use DBIx::Connection::Oracle::SQL;


=head1 DESCRIPTION

    Represents sql abstract layer

=head1 EXPORT

None

=head2 METHODS

=over

=item sequence_value

Returns sql statement that returns next sequence value

=cut

sub sequence_value {
    my ($class, $sequence_name) = @_;
    "SELECT ${sequence_name}.NEXTVAL as val FROM dual"
}



=item reset_sequence

Returns sql statement that restarts sequence.

=cut

sub reset_sequence {
    my ($class, $sequence_name, $restart_with, $increment_by) = @_;
    $restart_with ||= 1;
    $increment_by ||= 1;
    ("DROP SEQUENCE ${sequence_name}", "CREATE SEQUENCE ${sequence_name} START WITH ${restart_with} INCREMENT BY ${increment_by}");
}


=item has_sequence

Returns sql statement that check is sequence exists in database schema

=cut

sub has_sequence {
    my ($class) = @_;
    "SELECT sequence_name FROM user_sequences WHERE sequence_name = UPPER(?)"
}


=item has_table

Returns sql statement that check is table exists in database schema

=cut

sub has_table {
    my ($class) = @_;
    "SELECT table_name FROM user_tables WHERE table_name = UPPER(?)";
}


=item primary_key_info

=cut

sub primary_key_info {
    my ($class, $schema) = @_;
    $schema
        ? "SELECT LOWER(cl.column_name) AS column_name, cs.constraint_name AS pk_name, LOWER(cs.table_name) AS table_name FROM all_cons_columns cl
JOIN all_constraints cs
ON (cl.owner = cs. owner AND cl.constraint_name = cs. constraint_name AND  constraint_type='P'
AND cs.table_name = UPPER(?) AND cs.owner = UPPER(?))
ORDER BY position"
        : "SELECT LOWER(cl.column_name) AS column_name,  cs.constraint_name, LOWER(cs.table_name) AS table_name FROM user_cons_columns cl
JOIN user_constraints cs
ON (cl.constraint_name = cs. constraint_name AND  constraint_type='P' AND cs.table_name = UPPER(?))
ORDER BY position";
}


1;    

__END__

=back

=head1 SEE ALSO

L<DBIx::Connection>

=head1 COPYRIGHT AND LICENSE

The DBIx::Connection::Oracle::SQL module is free software. You may distribute under the terms of
either the GNU General Public License or the Artistic License, as specified in
the Perl README file.

=head1 AUTHOR

Adrian Witas, E<lt>adrian@webapp.strefa.pl</gt>

=cut

1;
