package DBIx::Connection::PostgreSQL::SQL;

use strict;
use warnings;
use vars qw($VERSION $LOB_MAX_SIZE);

use Abstract::Meta::Class ':all';
use Carp 'confess';

$VERSION = 0.04;

$LOB_MAX_SIZE = (1024 * 1024 * 1024);

=head1 NAME

DBIx::Connection::PostgreSQL::SQL - PostgreSQL catalog sql abstractaction layer.

=cut  

=head1 SYNOPSIS
    
    use DBIx::Connection::PostgreSQL::SQL;

=head1 EXPORT

None

=head1 DESCRIPTION

    Represents sql abstractaction layer

=head2 ATTRIBUTES

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
    
    tables_info => q{
    SELECT
        pc.relname AS table_name
    FROM pg_class pc
    JOIN pg_namespace n ON n.oid = pc.relnamespace AND n.nspname = '%s' AND pc.relkind = 'r' 
    },

    schema_tables_info => q{
    SELECT
        pc.relname AS table_name
    FROM pg_class pc
    JOIN pg_authid pa ON pa.oid = pc.relowner AND pa.rolname = '%s'
    JOIN pg_namespace n ON n.oid = pc.relnamespace AND n.nspname = '%s' AND pc.relkind = 'r' 
    },

    table_info => q{
    SELECT
        pc.relname AS table_name
    FROM pg_class pc
    JOIN pg_authid pa ON pa.oid = pc.relowner AND pa.rolname = '%s'
    JOIN pg_namespace n ON n.oid = pc.relnamespace AND n.nspname = '%s'
    AND pc.relname  = ? AND pc.relkind = 'r' 
    },
    
    column_info => q{
    SELECT 
        pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS defval,
        format_type(att.atttypid,att.atttypmod) AS typname, 
        description
    FROM pg_attribute att
    JOIN pg_class cl ON cl.oid = att.attrelid
    JOIN pg_authid pa ON pa.oid = cl.relowner AND pa.rolname = '%s'
    JOIN pg_namespace n ON n.oid = cl.relnamespace AND n.nspname = '%s'
    LEFT OUTER JOIN pg_attrdef def ON adrelid = attrelid AND adnum = attnum
    LEFT OUTER JOIN pg_description des ON des.objoid = attrelid AND des.objsubid=attnum 
    WHERE cl.relname = ? AND att.attname = ? },
    
    index_info => q{ 
    SELECT 
        cls.relname AS index_name,
        tab.relname AS table_name, 
        att.attname AS column_name, 
        att.attnum AS position,
        idx.indisunique AS is_unique,
        idx.indisprimary AS is_pk,
        idx.indisclustered AS is_clustered,
        pg_get_expr(trim(BOTH '()' FROM idx.indexprs), idx.indrelid) AS expr,
        am.amname AS index_type,
        n.nspname AS schema_name
    FROM pg_index idx
    JOIN pg_class cls ON cls.oid=indexrelid
    JOIN pg_authid pa ON pa.oid = cls.relowner AND pa.rolname = '%s'
    JOIN pg_namespace n ON n.oid=cls.relnamespace AND n.nspname = '%s'
    JOIN pg_class tab ON tab.oid=indrelid
    JOIN pg_attribute att ON att.attrelid = idx.indexrelid
    JOIN pg_am am ON am.oid=cls.relam
    WHERE cls.relname = ? },
    
    unique_index_column => q{
    SELECT 
        cls.relname AS index_name,
        tab.relname AS table_name, 
        att.attname, 
        cls.relname,
        pa.rolname
    FROM pg_index idx
    JOIN pg_class cls ON cls.oid=indexrelid
    JOIN pg_authid pa ON pa.oid = cls.relowner AND pa.rolname = '%s'
    JOIN pg_namespace n ON n.oid = cls.relnamespace AND n.nspname = '%s'
    JOIN pg_class tab ON tab.oid = indrelid
    JOIN pg_attribute att ON att.attrelid = idx.indexrelid
    WHERE idx.indisunique = 't' AND att.attnum = 1
    AND tab.relname = ? AND att.attname = ? 
    },
    
    foreign_key_info => q {
    SELECT 
        con.conname AS fk_name,
        att.attname AS fk_column_name, 
        att.attnum AS fk_position,
        tab.relname AS fk_table_name,
        array_to_string(con.conkey,',') AS fk_order,
        n.nspname AS fk_schema,
        reftab.relname AS pk_table_name,
        refcon.conname AS pk_name,
        refatt.attname AS pk_column_name,
        refatt.attnum AS pk_position,
        array_to_string(refcon.conkey,',')  AS pk_order,
        n1.nspname AS pk_schema
    FROM pg_attribute att
    JOIN pg_class tab ON att.attrelid = tab.oid
    JOIN pg_authid pa ON pa.oid = tab.relowner AND pa.rolname = '%s'
    JOIN pg_namespace n ON n.oid = tab.relnamespace AND n.nspname = '%s'
    JOIN pg_constraint con ON con.conrelid = tab.oid AND con.contype = 'f' AND att.attnum = ANY (con.conkey)
    JOIN pg_class reftab ON con.confrelid  = reftab.oid
    JOIN pg_namespace n1 ON n1.oid = reftab.relnamespace AND n1.nspname = '%s'
    JOIN pg_constraint refcon ON refcon.conrelid = reftab.oid  AND refcon.contype = 'p' 
    AND refcon.conkey = con.confkey
    JOIN pg_attribute refatt ON refatt.attrelid = reftab.oid AND refatt.attnum = ANY (refcon.conkey)
    WHERE tab.relname = ? AND reftab.relname = ? },

    trigger_info => q{
    SELECT 
        t.tgname AS trigger_name, 
        relname AS table_name, 
        nspname AS trigger_schema, 
        des.description,
        p.proname AS trigger_func,
        p.prosrc as trigger_body
    FROM pg_trigger t
    JOIN pg_class cl ON cl.oid=tgrelid
    JOIN pg_authid pa ON pa.oid = cl.relowner AND pa.rolname = '%s'
    JOIN pg_namespace na ON na.oid = cl.relnamespace AND na.nspname = '%s'
    LEFT OUTER JOIN pg_description des ON des.objoid=t.oid
    LEFT OUTER JOIN pg_proc p ON p.oid=t.tgfoid
    LEFT OUTER JOIN pg_language l ON l.oid=p.prolang
    WHERE NOT tgisconstraint AND t.tgname = ?
    },
    
    routine_info => q{
    SELECT
        pc.oid AS routine_id,
        pc.proname AS routine_name,
        pc.prosrc AS routine_body,
        nspname AS routine_schema,
        rtp.typname as return_type,
        pc.proargtypes,
        tp.typname,
        array_to_string(pc.proallargtypes, ',') AS routine_args_type,
        tp.oid AS type_id,
        array_to_string(pc.proargmodes,',') AS routine_args_mode,
        array_to_string(pc.proargnames, ',') AS routine_args_name
   FROM pg_proc pc
   JOIN pg_type tp ON tp.oid =  ANY (pc.proallargtypes)
   LEFT JOIN pg_type rtp ON rtp.oid = pc.prorettype
   JOIN pg_authid pa ON pa.oid = pc.proowner AND pa.rolname = '%s'
   JOIN pg_namespace na ON na.oid = pc.pronamespace AND na.nspname = '%s'
   WHERE pc.proname = ?
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
    JOIN pg_authid pa ON pa.oid = pc.relowner
    AND pc.relkind = 'S'
    AND  lower(pc.relname) = lower(?)
    AND rolname = '". $schema ."' ";
}


=item set_session_variables

Sets session variables

It uses the following sql command pattern:

    SET variable TO value;

    DBIx::Connection::PostgreSQL::SQL->set_session_variables($connection, {DateStyle => 'US'});

=cut

sub set_session_variables {
    my ($class, $connection, $db_session_variables) = @_;
    my $sql = "";
    $sql .= "SET " . $_ . " TO " . $db_session_variables->{$_} . ";"
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
    
    $class->_unlink_lob($connection, $class->_get_lob_id($connection, $table_name, $primary_key_values, $lob_column_name));
    my $lob_id = $class->_create_lob($connection, $lob);
    my $bind_counter = 1;
    my $sth = $connection->dbh->prepare($sql);
    $sth->bind_param($bind_counter++ ,$lob_id);
    $sth->bind_param($bind_counter++ , length($lob)) if $lob_size_column_name;
    for my $k (sort keys %$primary_key_values) {
        $sth->bind_param($bind_counter++ , $primary_key_values->{$k});
    }
    $sth->execute();
}



=item fetch_lob

Retrieve lobs.
Takes connection object, table name, lob column_name, hash_ref to primary key values. optionally lob size column name.
By default max lob size is set to 1 GB
DBIx::Connection::Oracle::SQL::LOB_MAX_SIZE = (1024 * 1024 * 1024);

=cut

sub fetch_lob {
    my ($class, $connection, $table_name, $lob_column_name, $primary_key_values, $lob_size_column_name) = @_;
    confess "missing primary key for lob update on ${table_name}.${lob_column_name}"
        if (! $primary_key_values  || ! (%$primary_key_values));
    confess "missing lob size column name" unless $lob_size_column_name;
    my $lob_id = $class->_get_lob_id($connection, $table_name, $primary_key_values, $lob_column_name);
    my $lob_size = $class->_get_lob_size($connection, $table_name, $primary_key_values, $lob_size_column_name);
    $class->_read_lob($connection, $lob_id, $lob_size);
}



=item _create_lob

Creates lob

=cut

sub _create_lob {
    my ($class, $connection, $lob) = @_;
    my $dbh = $connection->dbh;
    my $autocomit_mode = $connection->has_autocomit_mode;
    $connection->begin_work if $autocomit_mode;
    my $mode = $dbh->{pg_INV_WRITE};
    my $lob_id = $dbh->func($mode, 'lo_creat')
        or confess "can't create lob ". $!;
    my $lobj_fd = $dbh->func($lob_id, $mode, 'lo_open');
    confess "can't open lob ${lob_id}". $!
        unless defined $lobj_fd;
    my $length = length($lob || '');
    my $offset = 0;
    while($length > 0) {
        $dbh->func($lobj_fd, $offset, 0, 'lo_lseek');
        my $nbytes = $dbh->func($lobj_fd, substr($lob, $offset, $length), $length, 'lo_write')
            or confess "can't write lob " . $!;
        $offset += $nbytes;
        $length -=  $nbytes;
    }
    $dbh->func($lobj_fd, 'lo_close');
    $connection->commit if ($autocomit_mode);
    $lob_id;
}
    

=item _read_lob

Reads lob

=cut

sub _read_lob {
    my ($class, $connection, $lob_id, $length) = @_;
    my $dbh = $connection->dbh;
    my $autocomit_mode = $connection->has_autocomit_mode;
    $connection->begin_work if $autocomit_mode;
    my $mode = $dbh->{pg_INV_READ};
    my $lobj_fd = $dbh->func($lob_id, $mode, 'lo_open');
    confess "can't open lob ${lob_id}". $!
        unless defined $lobj_fd;
    my $result = '';
    my $buff = '';
    my $offset = 0;
    while(1) {
        $dbh->func($lobj_fd, $offset, 0, 'lo_lseek');
        my $nbytes = $dbh->func($lobj_fd, $buff, $length, 'lo_read');
        confess "can't read lob ${lob_id}" . $!
            unless defined $nbytes;
        $result .= $buff;
        $offset += $nbytes;
        last if ($offset == $length);
    }
    $dbh->func($lobj_fd, 'lo_close');
    $connection->commit if ($autocomit_mode);
    $result
}
    

=item _unlnik_lob

Removes lob.

=cut

sub _unlink_lob {
    my ($class, $connection, $lob_id) = @_;
    return unless  $lob_id;
    my $dbh = $connection->dbh;
    my $autocomit_mode = $connection->has_autocomit_mode;
    $connection->begin_work if $autocomit_mode;
    $dbh->func($lob_id, 'lo_unlink')
        or confess "can't unlink lob ${lob_id}". $!;
    $connection->commit if ($autocomit_mode);
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


=item _get_lob_id

Returns lob oid.

=cut

sub _get_lob_id {
    my ($class, $connection, $table_name, $primary_key_values, $lob_column_name) = @_;
    my $resut;
    my $sql = "SELECT ${lob_column_name} as lob_id FROM ${table_name} " . $connection->_where_clause($primary_key_values);
    my ($record) = $connection->record($sql, map { $primary_key_values->{$_}} sort keys %$primary_key_values);
    $resut = $record->{lob_id};
}


=item tables_info

=cut

sub tables_info {
    my ($self, $connection, $table, $schema) = @_;
    $schema ||= 'public';
    my $user = lc($connection->username);
    my $sql= $user
        ? sprintf($sql{schema_tables_info}, lc($connection->username), lc $schema)
        : sprintf($sql{tables_info}, lc $schema);
    my $sth = $connection->query_cursor(sql => $sql);
    my $resultset = $sth->execute();
    my $result = [];
    while ($sth->fetch()) {
        push @$result, {%$resultset};
    }
    $result;
}



=item table_info

=cut

sub table_info {
    my ($self, $connection, $table, $schema) = @_;
    $schema ||= 'public';
    my $sql= sprintf($sql{table_info}, lc($connection->username), lc $schema);
    my $sth = $connection->query_cursor(sql => $sql);
    my $resultset = $sth->execute([$table]);
    my $result = [];
    while ($sth->fetch()) {
        push @$result, [%$resultset]->[-1];
    }
    $result;
}


=item column_info

=cut

sub column_info {
    my ($self, $connection, $table, $column, $schema, $result) = @_;
    $schema ||= 'public';
    my $sql = sprintf($sql{column_info}, lc($connection->username), lc $schema);
    my $record = $connection->record($sql, $table, $column);
    $result->{default} = $record->{defval};
    $result->{db_type} = $record->{typname};
    $self->unique_index_column($connection, $table, $column, $schema, $result);
}


=item unique_index_column

=cut

sub unique_index_column {
    my ($self, $connection, $table, $column, $schema, $result) = @_;
    $schema ||= 'public';
    my $sql = sprintf($sql{unique_index_column}, lc($connection->username), lc $schema);
    my $record = $connection->record($sql, $table, $column);
    $result->{unique} = !! ($record->{index_name});
}


=item index_info

=cut

sub index_info {
    my ($self, $connection, $index, $schema) = @_;
    $schema ||= 'public';
    my $sql = sprintf($sql{index_info}, lc($connection->username), lc($schema));
    my $cursor = $connection->query_cursor(sql => $sql);
    my $record = $cursor->execute([$index]);
    my @result;
    while($cursor->fetch) {
        my $expr = $record->{expr};
        if($expr) {
            $expr =~ s/::[^,]+(,)/$1/g;
            $expr =~ s/::[^\)]+(\))/$1/g;
            $record->{column_name} = $expr;
        }
        push @result, {%$record};
    }
    return \@result;
}


=item foreign_key_info

PKTABLE_CAT ( UK_TABLE_CAT ): The primary (unique) key table catalog identifier. This field is NULL (undef) if not applicable to the data source, which is often the case. This field is empty if not applicable to the table.
PKTABLE_SCHEM ( UK_TABLE_SCHEM ): The primary (unique) key table schema identifier. This field is NULL (undef) if not applicable to the data source, and empty if not applicable to the table.
PKTABLE_NAME ( UK_TABLE_NAME ): The primary (unique) key table identifier.
PKCOLUMN_NAME (UK_COLUMN_NAME ): The primary (unique) key column identifier.
FKTABLE_CAT ( FK_TABLE_CAT ): The foreign key table catalog identifier. This field is NULL (undef) if not applicable to the data source, which is often the case. This field is empty if not applicable to the table.
FKTABLE_SCHEM ( FK_TABLE_SCHEM ): The foreign key table schema identifier. This field is NULL (undef) if not applicable to the data source, and empty if not applicable to the table.
FKTABLE_NAME ( FK_TABLE_NAME ): The foreign key table identifier.
FKCOLUMN_NAME ( FK_COLUMN_NAME ): The foreign key column identifier.
KEY_SEQ ( ORDINAL_POSITION ): The column sequence number (starting with 1).
UPDATE_RULE ( UPDATE_RULE ): The referential action for the UPDATE rule. The following codes are defined:
DELETE_RULE ( DELETE_RULE ): The referential action for the DELETE rule. The codes are the same as for UPDATE_RULE.
FK_NAME ( FK_NAME ): The foreign key name.
PK_NAME ( UK_NAME ): The primary (unique) key name.
DEFERRABILITY ( DEFERABILITY ): T

=cut

sub foreign_key_info {
    my ($self, $connection, $table_name, $reference_table_name, $schema, $reference_schema) = @_;
    $schema ||= 'public';
    $reference_schema ||= 'public';
    my $sql = sprintf($sql{foreign_key_info}, lc($connection->username), lc($schema), lc($reference_schema));
    my $cursor = $connection->query_cursor(sql => $sql);
    my $record = $cursor->execute([$table_name, $reference_table_name]);
    my (%pk, %fk, @pk_order, @fk_order, @result);
    while ($cursor->fetch) {
        @pk_order = grep { $_ }(split /,/, $record->{pk_order} . ",")
            unless @pk_order;
        @fk_order = grep { $_ }(split /,/, $record->{fk_order} . ",")
            unless @fk_order;
        $fk{$record->{fk_position}} = {%$record};
        $pk{$record->{pk_position}} = $record->{pk_column_name};
    }
    
    for my $i(0 .. $#fk_order)  {
        my $row = $fk{$fk_order[$i]};
        push @result, [
            undef,
            $row->{pk_schema},
            $row->{pk_table_name},
            $pk{$pk_order[$i]},
            undef, 
            $row->{fk_schema},
            $row->{fk_table_name},
            $row->{fk_column_name},
            $i + 1,
            undef,
            undef,
            $row->{fk_name},
            $row->{pk_name},
        ];
    }
    return \@result;
}


=item trigger_info

=cut

sub trigger_info {
    my ($self, $connection, $trigger, $schema) = @_;
    $schema ||= 'public';
    my $sql = sprintf($sql{trigger_info}, lc($connection->username), lc($schema));
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
    $schema ||= 'public';
    my $sql = sprintf($sql{routine_info}, lc($connection->username), lc($schema));
    my $cursor = $connection->query_cursor(sql => $sql);
    my $record = $cursor->execute([$routine]);
    my $routines = {};
    my %types;
    while ($cursor->fetch) {
        $types{$record->{type_id}} = $record->{typname};
        $routines->{$record->{routine_id}}  = {%$record};
    }
    my $result;
    my @args;
    if(%$routines)  {
        $result = [];
        foreach my $id (keys %$routines) {
            my $routine = $routines->{$id};
            my @args_mode = (split /,/, $routine->{routine_args_mode} .',');
            my @args_name = (split /,/, $routine->{routine_args_name} .',');
            my @args_type = (split /,/, $routine->{routine_args_type} .',');
            for my $i(0 .. $#args_name) {
                my $mode = $args_mode[$i];
                push @args, {
                    name => $args_name[$i],
                    type => $types{$args_type[$i]},
                    mode => ($mode eq 'i' ? 'IN' :
                                ($mode eq 'o' ? 'OUT' : 'INOUT'))
                };
            }
            my $declaration = join ', ', map { $_->{mode} . ' ' . $_->{name} . ' ' . $_->{type} } @args;
            push @$result, {
                routine_name   => $routine->{routine_name},
                routine_body   => $routine->{routine_body},
                routine_type   => 'FUNCTION',
                routine_arguments => $declaration,
                routine_schema => $routine->{routine_schema},
                return_type     => $routine->{return_type},
                args            =>  \@args
            }
        }
    }
    return $result;
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
