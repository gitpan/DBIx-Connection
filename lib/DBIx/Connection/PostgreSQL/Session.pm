package DBIx::Connection::PostgreSQL::Session;

use warnings;
use strict;

use vars qw($VERSION);

$VERSION = 0.02;

=head1 NAME

DBIx::Connection::PostgreSQL::Session - PostgreSQL session wrapper.

=head1 SYNOPSIS

DBIx::Connection::PostgreSQL::Session->initialise_session($connection, {DateStyle => 'US'});

=head1 DESCRIPTION

Initialise Oracle session's variables.

=head2 methods

=over

=item initialise_session

Iniitialise session variables

It uses the following sql command pattern:
    
    SET variable TO value;

=cut

sub initialise_session {
    my ($class, $connection, $db_session_variables) = @_;
    my $sql = "";
    $sql .= "SET " . $_ . " TO " . $db_session_variables->{$_} . ";"
      for keys %$db_session_variables;
    $connection->do($sql);
}

1;

__END__

=back

=head1 COPYRIGHT

The DBIx::Connection::Oracle::Session module is free software. You may distribute under the terms of
either the GNU General Public License or the Artistic License, as specified in
the Perl README file.

=head1 AUTHOR

Adrian Witas, adrian@webapp.strefa.pl

See also B<DBIx::Connection>.

=cut