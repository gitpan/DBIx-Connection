package DBIx::Connection::MySQL::SQL;

use strict;
use warnings;
use vars qw($VERSION);

use Abstract::Meta::Class ':all';
use Carp 'confess';

$VERSION = 0.06;

=head1 NAME

DBIx::Connection::MySQL::SQL - MySQL catalog sql abstract action layer.

=cut  

=head1 SYNOPSIS
    
    use DBIx::Connection::MySQL::SQL;


=head1 DESCRIPTION

    Represents sql abstractaction layer

=head1 EXPORT

None

=head2 METHODS

=over

=item sql

Stores definition of the following sql
 
 - column_info
 - index_info
 - unique_index_column
 - foreign_key_info
 - trigger_info
 - routine_info
 
=cut

our %sql = (

    column_info => q{
        SELECT 
         c.column_name,
         c.column_default AS defval,
         c.column_type AS typname,
         c.column_comment AS description,
         c.table_schema 
        FROM information_schema.COLUMNS c
        WHERE c.table_schema = '%s' AND lower(c.table_name) = ? AND lower(c.column_name) = ?
    },
    
    unique_index_column => q{
    SELECT
        k.column_name
    FROM information_schema.KEY_COLUMN_USAGE k
    JOIN information_schema.TABLE_CONSTRAINTS c 
	ON c.constraint_name = k.constraint_name 
	AND c.constraint_schema = k.constraint_schema AND k.constraint_schema = '%s'
     WHERE c.constraint_type = 'UNIQUE' AND k.ordinal_position = 1
     AND k.table_name = ? AND k.column_name = ?
    },
    
    foreign_key_info => q {
    SELECT 
        c.constraint_name AS fk_name,
        c.column_name AS fk_column_name, 
        c.ordinal_position AS fk_position,
        c.table_name AS fk_table_name,
        
        ct.constraint_name AS pk_name,
        ct.column_name AS pk_column_name, 
        ct.ordinal_position AS pk_position,
        ct.table_name AS pk_table_name
    FROM information_schema.KEY_COLUMN_USAGE c
    JOIN information_schema.TABLE_CONSTRAINTS t ON t.constraint_name = c.constraint_name 
    AND t.constraint_schema = c.constraint_schema AND  t.CONSTRAINT_TYPE = 'FOREIGN KEY' AND c.constraint_schema = '%s'
    JOIN information_schema.KEY_COLUMN_USAGE ct ON ct.table_name = c.referenced_table_name 
    AND ct.table_schema = c.referenced_table_schema AND c.ordinal_position = ct.ordinal_position AND ct.constraint_schema = '%s'
    AND ct.constraint_name = 'PRIMARY'
    WHERE c.table_name = ? AND ct.table_name = ?
    },

    table_foreign_key_info => q{
    SELECT 
        c.constraint_name AS fk_name,
        c.column_name AS fk_column_name, 
        c.ordinal_position AS fk_position,
        c.table_name AS fk_table_name,
        
        ct.constraint_name AS pk_name,
        ct.column_name AS pk_column_name, 
        ct.ordinal_position AS pk_position,
        ct.table_name AS pk_table_name
    FROM information_schema.KEY_COLUMN_USAGE c
    JOIN information_schema.TABLE_CONSTRAINTS t ON t.constraint_name = c.constraint_name 
    AND t.constraint_schema = c.constraint_schema AND  t.CONSTRAINT_TYPE = 'FOREIGN KEY' AND c.constraint_schema = '%s'
    JOIN information_schema.KEY_COLUMN_USAGE ct ON ct.table_name = c.referenced_table_name 
    AND ct.table_schema = c.referenced_table_schema AND c.ordinal_position = ct.ordinal_position AND ct.constraint_schema = c.constraint_schema 
    AND ct.constraint_name = 'PRIMARY'
    WHERE c.table_name = ?
    },
    
    index_info => q{     
        show index FROM %s FROM %s WHERE lower(key_name) = '%s'
    },

    table_indexex_info => q{     
        show index FROM %s FROM %s
    },
    
    trigger_info => q{
    SELECT 
        t.trigger_name,
        t.trigger_schema,
        t.event_object_table AS table_name,
        t.action_statement AS trigger_body
    FROM  information_schema.TRIGGERS t
    WHERE t.trigger_schema = '%s' AND t.trigger_name = ?
    },
    
    
    routine_info => q{
        SELECT 
        r.specific_name AS routine_name,
        r.routine_schema,
        r.routine_definition AS routine_body,
        r.routine_type
        FROM information_schema.ROUTINES r
        WHERE r.routine_schema ='%s' AND r.specific_name = ?
    },
    
    routing_additional_info => q{
        SHOW CREATE %s %s.%s;
    }
    
);

=item sequence_value

Returns sql statement that returns next sequence value

=cut

sub sequence_value {
    my ($self) = @_;
    confess "not supported";
}


=item reset_sequence

Returns sql statement that restarts sequence.

=cut

sub reset_sequence {
    my ($class, $name, $start_with, $increment_by, $connection) = @_;
    $connection->do("ALTER TABLE $name AUTO_INCREMENT = ${start_with}");
    ();
}


=item set_session_variables

Iniitialise session variable.
It uses the following command pattern:

    SET @@local.variable = value;

=cut

sub set_session_variables {
    my ($class, $connection, $db_session_variables) = @_;
    my $sql = "";
    $sql .= 'SET @@local.' . $_ . " = " . $db_session_variables->{$_} . ";"
      for keys %$db_session_variables;
    $connection->do($sql);
}


=item update_lob

Updates lob. (Large Object)
Takes connection object, table name, lob column_name, lob content, hash_ref to primary key values. optionally lob size column name.

=cut

sub update_lob {
    my ($class, $connection, $table_name, $lob_column_name, $lob, $primary_key_values, $lob_size_column_name) = @_;
    confess "missing primary key for lob update on ${table_name}.${lob_column_name}"
        if (!$primary_key_values  || ! (%$primary_key_values));
    confess "missing lob size column name" unless $lob_size_column_name;
    my $sql = "UPDATE ${table_name} SET ${lob_column_name} = ? ";
    $sql .= ($lob_size_column_name ? ", ${lob_size_column_name} = ? " : '')
      . $connection->_where_clause($primary_key_values);

    $connection->dbh->{max_allowed_packet} = length($lob) if $lob;
    my $bind_counter = 1;
    my $sth = $connection->dbh->prepare($sql);
    $sth->bind_param($bind_counter++ ,$lob);
    $sth->bind_param($bind_counter++ , ($lob ? length($lob) : 0)) if $lob_size_column_name;
    for my $k (sort keys %$primary_key_values) {
        $sth->bind_param($bind_counter++ , $primary_key_values->{$k});
    }
    $sth->execute();
    
    
}


=item fetch_lob

Retrieves lob.
Takes connection object, table name, lob column_name, hash_ref to primary key values

=cut

sub fetch_lob {
    my ($class, $connection, $table_name, $lob_column_name, $primary_key_values) = @_;
    confess "missing primary key for lob update on ${table_name}.${lob_column_name}"
        if (! $primary_key_values  || ! (%$primary_key_values));
    my $sql = "SELECT ${lob_column_name} as lob_content FROM ${table_name} " . $connection->_where_clause($primary_key_values);
    my $record = $connection->record($sql, map { $primary_key_values->{$_}} sort keys %$primary_key_values);
    $record->{lob_content};
}


=item tables_info

=cut

sub tables_info {
    my ($self, $connection, $schema) = @_;
    my $sth = $connection->query_cursor(sql => "SHOW TABLES ". ($schema ? " FROM $schema" : ""));
    my $resultset = $sth->execute();
    my $result = [];
    while ($sth->fetch()) {
        push @$result, {table_name => [%$resultset]->[-1]};
    }
    $result;
}


=item index_info

=cut

sub index_info {
    my ($self, $connection, $index, $schema, $table) = @_;
    return undef
        unless $table;
    return unless $connection->has_table($table);
    $schema ||= $connection->username;
    my $sql = sprintf($sql{index_info}, lc($table), lc($connection->username), lc($index));
    my $cursor = $connection->query_cursor(sql => $sql);
    my $record = $cursor->execute([]);
    my @result;
    while($cursor->fetch) {
        push @result, {
            index_name   => $record->{key_name},
            table_name   => $record->{table},
            column_name  => $record->{column_name},
            position     => $record->{seq_in_index},
            is_unique    => ! $record->{non_unique},
            is_pk        => 0,
            is_clustered => 0,
        };
    }
    return \@result;
}

=item table_indexes_info

=cut

sub table_indexes_info {
    my ($self, $connection, $table, $schema) = @_;
    return undef
        unless $table;
    return unless $connection->has_table($table);
    $schema ||= $connection->username;
    my $sql = sprintf($sql{table_indexex_info}, lc($table), lc($connection->username));
    my $cursor = $connection->query_cursor(sql => $sql);
    my $record = $cursor->execute([]);
    my %result;
    while($cursor->fetch) {
        push @{$result{$record->{key_name}}}, {
            index_name   => $record->{key_name},
            table_name   => $record->{table},
            column_name  => $record->{column_name},
            position     => $record->{seq_in_index},
            is_unique    => ! $record->{non_unique},
            is_pk        => 0,
            is_clustered => 0,
        };
    }
    return %result ? [values %result] : undef;
}


=item column_info

=cut

sub column_info {
    my ($self, $connection, $table, $column, $schema, $result) = @_;
    $schema ||= $connection->username;
    my $sql = sprintf($sql{column_info}, lc $schema);
    my $record = $connection->record($sql, lc($table), lc $column);
    $result->{default} = $record->{defval};
    $result->{db_type} = $record->{typname};
    $self->unique_index_column($connection, $table, $column, $schema, $result);
       
}


=item unique_index_column

=cut

sub unique_index_column {
    my ($self, $connection, $table, $column, $schema, $result) = @_;
    $schema ||= $connection->username;
    my $sql = sprintf($sql{unique_index_column}, lc $schema);
    my $record = $connection->record($sql, $table, $column);
    $result->{unique} = !! ($record->{column_name});
}


=item foreign_key_info

=cut

sub foreign_key_info {
    my ($self, $connection, $table_name, $reference_table_name, $schema, $reference_schema) = @_;
    $schema ||= $connection->username;
    $reference_schema ||= $connection->username;
    my $sql = sprintf($sql{foreign_key_info}, lc($schema), lc($reference_schema));
    my $cursor = $connection->query_cursor(sql => $sql);
    my $record = $cursor->execute([$table_name, $reference_table_name]);
    my @result;
    
    while ($cursor->fetch) {
        push @result, [
            undef,
            $record->{pk_schema},
            $record->{pk_table_name},
            $record->{pk_column_name},
            undef, 
            $record->{fk_schema},
            $record->{fk_table_name},
            $record->{fk_column_name},
            $record->{fk_position},
            undef,
            undef,
            $record->{fk_name},
            $record->{pk_name},
        ];
    }
    return \@result;
}


=item table_foreign_key_info

=cut

sub table_foreign_key_info {
    my ($self, $connection, $table_name, $schema) = @_;
    $schema ||= $connection->username;
    my $sql = sprintf($sql{table_foreign_key_info}, lc($schema));
    my $cursor = $connection->query_cursor(sql => $sql);
    my $record = $cursor->execute([lc($table_name)]);
    my $owner = lc $connection->username;
    my %result;
    while ($cursor->fetch) {
        my $id = $record->{fk_name};
        push @{$result{$id}}, [
            undef,
            ($record->{pk_schema} || $owner),
            $record->{pk_table_name},
            $record->{pk_column_name},
            undef, 
            ($record->{fk_schema} || $owner),
            $record->{fk_table_name},
            $record->{fk_column_name},
            $record->{fk_position},
            undef,
            undef,
            $record->{fk_name},
            $record->{pk_name},
        ];
    }
    return %result ? [values %result] : undef;
}


=item trigger_info

=cut

sub trigger_info {
    my ($self, $connection, $trigger, $schema) = @_;
    $schema ||= $connection->username;
    my $sql = sprintf($sql{trigger_info}, lc($schema));
    my $cursor = $connection->query_cursor(sql => $sql);
    my $record = $cursor->execute([$trigger]);
    my $result;
    while ($cursor->fetch) {
        $result = {%$record};
    }
    return $result;
}


=item routine_info

Returns array of function info for the specified function name.

=cut

sub routine_info {
    my ($self, $connection, $routine, $schema) = @_;
    return unless $routine;
    $schema ||= $connection->username;
    my $sql = sprintf($sql{routine_info}, lc($schema));
    my $cursor = $connection->query_cursor(sql => $sql);
    my $record = $cursor->execute([$routine]);
    
    my $routines = {};
    my $result = [];
    while ($cursor->fetch) {
        my $additional_info = $connection->record(sprintf($sql{routing_additional_info}, $record->{routine_type}, lc($schema), $routine));
        my $create_procedure = $additional_info->{lc('create ' . $record->{routine_type})};
        my ($routine_arguments, $return) = ($create_procedure =~ /$routine[^\(]*\((.+)\)[^R]*RETURNS[^\w]+([\w\(\)\d]+)/imx);
        unless ($routine_arguments) {
            ($routine_arguments) = ($create_procedure =~ /$routine[^\(]*\((.+)\)[^B]*BEGIN/imx);
        }

        my @routine_args = split /,/, $routine_arguments .",";
        my @args = map { my $arg = $_;
            ($self->_parse_routine_argument($arg))
        } @routine_args;
        push @$result, {%$record,
            return_type => ($return || ''),
            routine_arguments => $routine_arguments,
            args => \@args
        };
    }
    
    @$result ? $result : undef;

}


=item _parse_routine_argument

=cut

sub _parse_routine_argument {
    my ($class, $arg) = @_;
    $arg =~ s/^\s+//;
    my @parts = split /\s/, $arg;
    my $result = {};
    if (@parts == 3) {
        $result->{mode} = shift @parts;
    }
    $result->{name} = $parts[0];
    $result->{type} = $parts[1];
    return $result;
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
