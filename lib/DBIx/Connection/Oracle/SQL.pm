package DBIx::Connection::Oracle::SQL;

use strict;
use warnings;
use vars qw($VERSION $LOB_MAX_SIZE);

use Abstract::Meta::Class ':all';
use Carp 'confess';

$VERSION = 0.05;
$LOB_MAX_SIZE = (1024 * 1024 * 1024);

=head1 NAME

DBIx::Connection::Oracle::SQL - Oracle catalog sql abstract action layer.

=cut  

=head1 SYNOPSIS
    
    use DBIx::Connection::Oracle::SQL;


=head1 EXPORT

None

=head1 DESCRIPTION

    Represents sql abstract layer

=head2 ATTRIBUTES

=over

=item sql

Stores definition of the following sql
 
 - column_info
 - schema_column_info 
 - basis_obejct_info
 - schema_obejct_info
 - unique_index_column
 - scheam_unique_index_column
 

 
=cut

our %sql = (
    basic_obejct_info => q{
        SELECT %s_name FROM user_%ss WHERE %s_name = UPPER(?)
    },
    
    schema_obejct_info => q{
        SELECT %s_name FROM all_%ss WHERE %s_name = UPPER(?) AND OWNER = '%s'
    },

    tables_info => q{
        SELECT table_name FROM user_tables
    },
    
    schema_tables_info => q{
        SELECT table_name FROM all_tables WHERE OWNER = '%s'
    },
    
    column_info => q{
        SELECT c.data_type 
        || (CASE WHEN c.char_col_decl_length IS NOT NULL THEN '(' ||c.char_col_decl_length || ')'
         WHEN c.data_precision IS NOT NULL THEN '(' ||c.data_precision || (CASE WHEN COALESCE(c.data_scale,0) > 0 THEN ',' ||c.data_scale END)|| ')'
        END) AS typname,
        c.column_name,
        c.default_length,
        (SELECT m.comments FROM user_col_comments m WHERE m.table_name =  c.table_name AND m.column_name =  c.column_name) AS description
        FROM user_tab_cols c
        WHERE c.table_name = UPPER(?) AND c.column_name = UPPER(?)
    },

    schema_column_info => q{
        SELECT c.data_type 
        || (CASE WHEN c.char_col_decl_length IS NOT NULL THEN '(' ||c.char_col_decl_length || ')'
         WHEN c.data_precision IS NOT NULL THEN '(' ||c.data_precision || (CASE WHEN COALESCE(c.data_scale,0) > 0 THEN ',' ||c.data_scale END)|| ')'
        END) AS typname,
        c.column_name,
        c.default_length,
        (SELECT m.comments FROM all_col_comments m
        WHERE m.table_name =  c.table_name AND m.column_name =  c.column_name AND c.owner = m.owner) AS description
        FROM all_tab_cols c WHERE c.owner = UPPER('%s') AND c.table_name = UPPER(?) AND c.column_name = UPPER(?)
    },
    
    index_info => q{ 
        SELECT 
            c.index_name,
            LOWER(c.table_name) AS table_name,
            LOWER(c.column_name) AS column_name,
            c.column_position,
            (CASE WHEN p.constraint_name IS NOT NULL THEN 1 ELSE 0 END) AS is_pk,
            (CASE WHEN i.uniqueness = 'UNIQUE' THEN 1 ELSE 0 END) AS is_unique,
            i.index_type
        FROM user_ind_columns c
        JOIN user_indexes i ON i.index_name = c.index_name
        LEFT JOIN user_constraints p ON (p.index_name = c.index_name AND p.constraint_type = 'P')
        WHERE c.index_name = ?
        ORDER BY c.column_position
    },

    schema_index_info => q{ 
        SELECT 
            c.index_name,
            LOWER(c.table_name) AS table_name,
            LOWER(c.column_name) AS column_name,
            c.column_position,
            (CASE WHEN p.constraint_name IS NOT NULL THEN 1 ELSE 0 END) AS is_pk,
            (CASE WHEN i.uniqueness = 'UNIQUE' THEN 1 ELSE 0 END) AS is_unique,
            i.index_type
        FROM all_ind_columns c
        JOIN all_indexes i ON i.index_name = c.index_name AND i.owner = c.index_owner AND i.owner = '%s'
        LEFT JOIN all_constraints p ON (p.index_name = c.index_name AND p.constraint_type = 'P' AND i.owner = p.owner)
        WHERE c.index_name = ?
        ORDER BY c.column_position
    },
    
    unique_index_column => q{
        SELECT 
            c.index_name,
            c.table_name,
            c.column_name,
            c.column_position
        FROM user_ind_columns c
        JOIN user_indexes i ON i.index_name = c.index_name
        WHERE i.uniqueness = 'UNIQUE' AND c.column_position = 1
        AND c.table_name = ? AND c.column_name = ?
        ORDER BY c.column_position
    },
    
    schema_unique_index_column => q{
        SELECT 
            c.index_name,
            c.table_name,
            c.column_name,
            c.column_position
        FROM all_ind_columns c
        JOIN all_indexes i ON i.index_name = c.index_name
        WHERE i.uniqueness = 'UNIQUE' AND c.column_position = 1 AND c.owner = UPPER('%s')
        AND c.table_name = ? AND c.column_name = ?
    },

    primary_key_info => q{
        SELECT
            LOWER(cl.column_name) AS column_name,
            cs.constraint_name,
            LOWER(cs.table_name) AS table_name
        FROM user_cons_columns cl
        JOIN user_constraints cs ON (cl.constraint_name = cs. constraint_name
         AND constraint_type='P'
         AND cs.table_name = UPPER(?))
        ORDER BY position        
    },
    
    schema_primary_key_info => q{
        SELECT
            LOWER(cl.column_name) AS column_name,
            cs.constraint_name AS pk_name,
            LOWER(cs.table_name) AS table_name
        FROM all_cons_columns cl
        JOIN all_constraints cs
         ON (cl.owner = cs. owner AND cl.constraint_name = cs. constraint_name AND  constraint_type='P'
         AND cs.table_name = UPPER(?) AND cs.owner = UPPER(?))
        ORDER BY position
    },
    
    foreign_key_info => q{    
       SELECT
            fk.constraint_name AS fk_name, 
            LOWER(fk.table_name) AS fk_table_name,
            LOWER(fkc.column_name) AS fk_column_name,
            fkc.position AS fk_position,
            pk.constraint_name AS pk_name, 
            LOWER(pk.table_name) AS pk_table_name,
            LOWER(pkc.column_name) AS pk_column_name,
            pkc.position AS pk_position
        FROM user_cons_columns fkc
        JOIN user_constraints fk ON (fkc.constraint_name = fk.constraint_name AND fk.constraint_type='R' AND fk.table_name = ? ) 
        JOIN user_constraints pk ON (pk.constraint_name = fk.r_constraint_name AND pk.constraint_type='P' AND pk.table_name = ?)
        JOIN user_cons_columns pkc ON (pkc.constraint_name = pk.constraint_name AND pkc.position = fkc.position)
        ORDER BY fkc.position
    },
    
    schema_foreign_key_info => q{
       SELECT
            fk.constraint_name AS fk_name, 
            LOWER(fk.table_name) AS fk_table_name,
            LOWER(fkc.column_name) AS fk_column_name,
            fkc.position AS fk_position,
            pk.constraint_name AS pk_name, 
            LOWER(pk.table_name) AS pk_table_name,
            LOWER(pkc.column_name) AS pk_column_name,
            pkc.position AS pk_position
        FROM all_cons_columns fkc
        JOIN all_constraints fk ON (fkc.constraint_name = fk.constraint_name AND fk.constraint_type='R' AND fk.table_name = ? AND fkc.owner = fk.owner AND fk.owner = '%s') 
        JOIN all_constraints pk ON (pk.constraint_name = fk.r_constraint_name AND pk.constraint_type='P' AND pk.table_name = ? AND pk.owner = '%s')
        JOIN all_cons_columns pkc ON (pkc.constraint_name = pk.constraint_name AND pkc.position = fkc.position AND pk.owner = pkc.owner)
        ORDER BY fkc.position
    },
    
    trigger_info => q{
        SELECT 
            t.trigger_name,
            t.table_name,
            t.trigger_type,
            t.triggering_event,
            t.table_owner AS trigger_schema
        FROM user_triggers t
        WHERE t.trigger_name = ?
    },
    
    schema_trigger_info => q {
        SELECT 
            t.trigger_name,
            t.table_name,
            t.trigger_type,
            t.triggering_event,
            t.table_owner AS trigger_schema
        FROM all_triggers t
        WHERE t.trigger_name = ? AND t.table_owner = '%s'
    },
    
    routine_info => q{
        SELECT  
            p.object_name AS routine_name,
            p.object_type AS routine_type,
            p.object_id AS  routine_id,
            s.text AS routine_body,
            s.line
        FROM  user_source s 
        JOIN user_procedures p ON s.name = p.object_name AND s.type = p.object_type
        WHERE p.object_name = ?
        ORDER BY routine_id, s.line
    }
);


=back

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
    my ($classm, $schema) = @_;
    my $type = 'sequence';
    return sprintf($sql{basic_obejct_info}, $type, $type, $type);
        
}


=item table_info

Returns sql statement that check is table exists in database schema

=cut

sub table_info {
    my ($class, $connection, $table_name, $schema, $type) = @_;
    my $result;
    $type = lc($type);
    my $sql = ! $schema 
        ? sprintf($sql{basic_obejct_info}, $type, $type, $type)
        : sprintf($sql{schema_obejct_info}, $type, $type, $type, $schema);
    my $record = $connection->record($sql, uc $table_name);
    $result = [undef,$connection->name, $record->{"${type}_name"}, undef]
        if $record->{"${type}_name"};
    $result 
}


=item tables_info

=cut

sub tables_info {
    my ($self, $connection, $table, $schema) = @_;
    $schema ||= 'public';
    my $sql= sprintf($sql{tables_info}, lc($connection->username), lc $schema);
    my $sth = $connection->query_cursor(sql => $sql);
    my $record = $sth->execute();
    my $result = [];
    while ($sth->fetch()) {
        push @$result, {%$record};
    }
    $result;
}


=item primary_key_info

=cut

sub primary_key_info {
    my ($class, $schema) = @_;
    $schema ? $sql{schema_primary_key_info} : $sql{primary_key_info};
}


=item foreign_key_info

=cut

sub foreign_key_info {
    my ($self, $connection, $table_name, $reference_table_name, $schema, $reference_schema) = @_;
    my $sql = $schema ? sprintf($sql{schema_foreign_key_info}, uc($schema), uc $reference_schema)
    : $sql{foreign_key_info};
    my $cursor = $connection->query_cursor(sql => $sql);
    my $record = $cursor->execute([uc ($table_name), uc($reference_table_name)]);
    my $owner = lc $connection->username;
    my @result;
    while ($cursor->fetch) {
        push @result, [
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
    return \@result;
}



=item column_info

=cut

sub column_info {
    my ($self, $connection, $table, $column, $schema, $result) = @_;
    my $sql = $schema ? sprintf($sql{'schema_column_info'}, uc $schema) : $sql{'column_info'};
    my $record = $connection->record($sql, $table, $column);
    $result->{db_type} = $record->{typname};
    $result->{default} = ! $record->{default_length} ? '' :
        $connection->fetch_lob(user_tab_cols => 'data_default', {column_name => uc($column), table_name => uc($table)}, 'default_length');
    $self->unique_index_column($connection, $table, $column, $schema, $result);
}


=item unique_index_column

=cut

sub unique_index_column {
    my ($self, $connection, $table, $column, $schema, $result) = @_;
    my $sql = $schema
    ? sprintf($sql{schema_unique_index_column}, $connection->username)
    : $sql{unique_index_column};
    my $record = $connection->record($sql, uc($table), uc $column);
    $result->{unique} = !! ($record->{index_name});
}


=item index_info

=cut

sub index_info {
    my ($self, $connection, $index, $schema) = @_;
    my $sql = $schema
        ?  sprintf($sql{schema_index_info}, uc $schema)
        :  $sql{index_info};
    my $cursor = $connection->query_cursor(sql => $sql);
    my $record = $cursor->execute([uc $index]);
    my @result;
    while($cursor->fetch) {
        push @result, {%$record};
    }
    return \@result;
}


=item trigger_info

=cut

sub trigger_info {
    my ($self, $connection, $trigger, $schema) = @_;
    my $sql = $schema
        ? sprintf($sql{schema_trigger_info}, uc($schema))
        : $sql{trigger_info};
        
    my $cursor = $connection->query_cursor(sql => $sql);
    my $record = $cursor->execute([uc $trigger]);
    my $result;
    while ($cursor->fetch) {
        $result = {%$record,
            trigger_body => $connection->fetch_lob(user_triggers => 'trigger_body', {trigger_name => uc($trigger)})
        };
    }
    return $result;
}


=item routine_info

Returns array of function info for the specified function name.

=cut

sub routine_info {
    my ($self, $connection, $routine, $schema) = @_;
    return unless $routine;
    $schema ||= 'public';
    my $sql = $sql{routine_info};
    my $cursor = $connection->query_cursor(sql => $sql);
    my $record = $cursor->execute([uc $routine]);
    my $routines = {};
    while ($cursor->fetch) {
        my $id = $record->{routine_id};
        if($routines->{$id}) {
            $routines->{$id}->{routine_body} .= $record->{routine_body};
            
        } else {
            $routines->{$id} = {%$record};
        }
    }
    my $result;
    if(%$routines) {
        $result = [];
        foreach my $routine_info (values %$routines) {
            my $create_routine = $routine_info->{routine_body};
            my ($routine_arguments, $return) = ($create_routine =~ /$routine[^\(]*\((.+)\)[^R]*RETURN[^\w]+(\w+)/imx);
            unless ($routine_arguments) {
                ($routine_arguments) = ($create_routine =~ /$routine[^\(]*\((.+)\)[^AI]*[AI]S/imx);
            }
            my @routine_args = split /,/, $routine_arguments .",";
            my @args = map { my $arg = $_;
                ($self->_parse_routine_argument($arg))
            } @routine_args;
            $routine_info->{return_type} = ($return || '');
            $routine_info->{routine_arguments} = join (', ', map { $_->{name} . ' ' . ($_->{mode} ? $_->{mode} . ' ' : '') . $_->{type}} @args);
            $routine_info->{args} = \@args;
            push @$result, $routine_info;
        }
    }
    return $result;
}


=item _parse_routine_argument

=cut

sub _parse_routine_argument {
    my ($class, $arg) = @_;
    $arg =~ s/^\s+//;
    my @parts = split /\s+/, $arg;
    my $result = {};
    if (@parts == 4) {
        $result->{mode} = join(' ', @parts[1..2]);
        @parts = @parts[0,3];
    } elsif (@parts == 3) {
        $result->{mode} = $parts[1];
        @parts = @parts[0,2];
    }
    
    $result->{name} = $parts[0];
    $result->{type} = $parts[1];
    return $result;
}

=item set_session_variables

Sets session variables.
It uses the following sql command pattern,

    alter session set variable  = value;

    DBIx::Connection::Oracle::Session->initialise_session($connection, {NLS_DATE_FORMAT => 'DD.MM.YYYY'});

=cut

sub set_session_variables {
    my ($class, $connection, $db_session_variables) = @_;
    my $plsql = "BEGIN\n";
    $plsql .= "execute immediate 'alter session set " . $_ . "=''" . $db_session_variables->{$_} . "''';\n" 
      for keys %$db_session_variables;
    $plsql .= "END;";
    $connection->do($plsql);
}


=item update_lob

Updates lob. (Large Object)
Takes connection object, table name, lob column_name, lob content, hash_ref to primary key values. optionally lob size column name.

=cut

sub update_lob {
    my ($class, $connection, $table_name, $lob_column_name, $lob, $primary_key_values, $lob_size_column_name) = @_;
    confess "missing primary key for lob update on ${table_name}.${lob_column_name}"
        if (!$primary_key_values  || ! (%$primary_key_values));

    my $sql = "UPDATE ${table_name} SET ${lob_column_name} = ? ";
    $sql .= ($lob_size_column_name ? ", ${lob_size_column_name} = ? " : '')
      . $connection->_where_clause($primary_key_values);
    my $clas = 'DBD::Oracle';
    my $ora_type = $clas->can('SQLT_BIN') ? $class->SQLT_BIN : $clas->ORA_BLOB;
    my $bind_counter = 1;
    my $sth = $connection->dbh->prepare($sql);
    $sth->bind_param($bind_counter++ ,$lob, { ora_type => $ora_type});
    $sth->bind_param($bind_counter++ , length($lob || '')) if $lob_size_column_name;
    for my $k (sort keys %$primary_key_values) {
        $sth->bind_param($bind_counter++ , $primary_key_values->{$k});
    }
    $sth->execute();
}


=item fetch_lob

Retrieves lob.
Takes connection object, table name, lob column_name, hash_ref to primary key values. optionally lob size column name.
By default max lob size is set to 1 GB
DBIx::Connection::Oracle::SQL::LOB_MAX_SIZE = (1024 * 1024 * 1024);

=cut

{
    my %long_read_cache;

    sub fetch_lob {
        my ($class, $connection, $table_name, $lob_column_name, $primary_key_values, $lob_size_column_name) = @_;
        confess "missing primary key for lob update on ${table_name}.${lob_column_name}"
            if (! $primary_key_values  || ! (%$primary_key_values));
    
        my $dbh = $connection->dbh;
        # a bit hacky but it looks like DBD::Oracle 1.20 caches first call with LongReadLen
        # and doesn't allow updates for greater size then the initial LongReadLen read
        # so physically 1GB on lob limitation to declared here variable $LOB_SIZE = (1024 * 1024 * 1024);
        # another working solution is to reconnection - too expensive though
        
        if (! exists($long_read_cache{"_" . $dbh})){
            $dbh->{LongReadLen} = $LOB_MAX_SIZE;
            $long_read_cache{"_" . $dbh} = 1;
            
        } else {
            $dbh->{LongReadLen} = $class->_get_lob_size($connection, $table_name, $primary_key_values, $lob_size_column_name);
        }
        
        my $sql = "SELECT ${lob_column_name} as lob_content FROM ${table_name} " . $connection->_where_clause($primary_key_values);
        my $record = $connection->record($sql, map { $primary_key_values->{$_}} sort keys %$primary_key_values);
        $record->{lob_content};
    }
}


=item _get_lob_size

Returns lob size.

=cut

sub _get_lob_size {
    my ($class, $connection, $table_name, $primary_key_values, $lob_size_column_name) = @_;
    my $resut;
    if($lob_size_column_name) {
        my $sql = "SELECT ${lob_size_column_name} as lob_size FROM ${table_name} " . $connection->_where_clause($primary_key_values);
        my ($record) = $connection->record($sql, map { $primary_key_values->{$_}} sort keys %$primary_key_values);
        $resut = $record->{lob_size};
    }
    $resut || $LOB_MAX_SIZE;
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

Adrian Witas, adrian@webapp.strefa.pl

=cut

