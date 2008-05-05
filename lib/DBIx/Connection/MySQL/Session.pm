package DBIx::Connection::MySQL::Session;

use warnings;
use strict;

use vars qw($VERSION);

$VERSION = 0.01;

=head1 NAME

DBIx::Connection::PostgreSQL::Session - sets MySQL session variables.

=head1 SYNOPSIS

DBIx::Connection::Oracle::Session->initialise_session($connection, {NLS_DATE_FORMAT => 'DD.MM.YYYY'});

=head1 DESCRIPTION

Initialise Oracle session's variables.

=head2 methods

=over

=item initialise_session

Iniitialise session.

=cut

sub initialise_session {
    my ($class, $connection, $db_session_variables) = @_;
    my $sql = "";
    $sql .= 'SET @@local.' . $_ . " = " . $db_session_variables->{$_} . ";"
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

Adrian Witas, E<lt>adrian@webapp.strefa.pl</gt>

See also B<DBIx::Connection>.

=cut