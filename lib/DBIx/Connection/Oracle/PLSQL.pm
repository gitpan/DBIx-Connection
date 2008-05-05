package DBIx::Connection::Oracle::PLSQL;

use warnings;
use strict;

use Abstract::Meta::Class ':all';
use Carp 'confess';
use base qw(DBIx::PLSQLHandler);

use vars qw($VERSION);

$VERSION = 0.01;


=head1 NAME

DBIx::Connection::Oracle::PLSQL - PLSQL block handler

=head1 SYNOPSIS

=head2 methods

=over

=item initialise

Initialises handler.

=cut 

sub prepare {
    my ($self) = @_;
    $self->set_sql($self->plsql);
    $self->SUPER::prepare();
}



=item execute

=cut

sub execute {
    my $self = shift;
    my $result_set = $self->bind_parameters(@_);
    $self->SUPER::execute();
    $result_set;
}


=item bind_parameters

=cut

sub bind_parameters {
    my ($self, %bind_parameters) = @_;
    my $bind_variables = $self->bind_variables;
    my @bind_in_variables = $self->bind_in_variables;
    my %bind_params = (map {$_ => $bind_parameters{$_}} @bind_in_variables);
    my @binded_out_variables = $self->binded_out_variables;
    my %bind_params_inout = (map {$_ => $bind_parameters{$_}} @binded_out_variables);
    $self->bind_params(\%bind_params);
    $self->bind_params_inout(\%bind_params_inout);
    \%bind_params_inout;
}

__END__

=back

=head1 COPYRIGHT

The DBIx::Connection::Oracle::PLSQL module is free software. You may distribute under the terms of
either the GNU General Public License or the Artistic License, as specified in
the Perl README file.

=head1 AUTHOR

Adrian Witas, E<lt>adrian@webapp.strefa.pl</gt>

See also B<DBIx::Connection> B<DBIx::QueryCursor> B<DBIx::SQLHandler>.

=cut