package DBIx::Connection::MySQL::SQL;

use strict;
use warnings;
use vars qw($VERSION);

use Abstract::Meta::Class ':all';
use Carp 'confess';

$VERSION = 0.01;

=head1 NAME

DBIx::Connection::MySQL::SQL - MySQL catalog sql abstractaction layer.

=cut  

=head1 SYNOPSIS
    
    use DBIx::Connection::MySQL::SQL;


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
    my ($self) = @_;
    confess "not supported";
}


=item restart_sequence

Returns sql statement that restarts sequence.

=cut

sub reset_sequence {
    my ($class, $name, $start_with, $increment_by, $connection) = @_;
    $connection->do("ALTER TABLE $name AUTO_INCREMENT = ${start_with}");
    ();
}



1;    

__END__

=back

=head1 SEE ALSO

L<DBIx::PLSQLHandler>

=head1 COPYRIGHT AND LICENSE

The DBIx::Connection::MySQL::SQL module is free software. You may distribute under the terms of
either the GNU General Public License or the Artistic License, as specified in
the Perl README file.

=head1 AUTHOR

Adrian Witas, adrian@webapp.strefa.pl

=cut

1;
