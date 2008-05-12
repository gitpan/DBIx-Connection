package DBIx::Connection::Oracle::Session;

use warnings;
use strict;

use vars qw($VERSION);

$VERSION = 0.02;

=head1 NAME

DBIx::Connection::Oracle::Session - Oracle session wrapper.

=head1 SYNOPSIS

DBIx::Connection::Oracle::Session->initialise_session($connection, {NLS_DATE_FORMAT => 'DD.MM.YYYY'});

=head1 DESCRIPTION

Initialise Oracle session's variables.

=head2 methods

=over

=item initialise_session

Iniitialise session variables.
It uses the following sql command pattern,

    alter session set variable  = value;

=cut

sub initialise_session {
    my ($class, $connection, $db_session_variables) = @_;
    my $plsql = "BEGIN\n";
    $plsql .= "execute immediate 'alter session set " . $_ . "=''" . $db_session_variables->{$_} . "''';\n" 
      for keys %$db_session_variables;
    $plsql .= "END;";
    $connection->do($plsql);
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