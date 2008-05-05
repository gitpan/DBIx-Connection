
use strict;
use warnings;

use Test::More tests => 6;

use DBIx::Connection;

BEGIN {
    use_ok('DBIx::SQLHandler');
}

SKIP: {
    # all tests assume that there is the following table CREATE TABLE test(id NUMBER, name VARCHAR2(100))
    skip('missing env varaibles DB_TEST_CONNECTION, DB_TEST_USERNAME DB_TEST_PASSWORD', 5)
      unless $ENV{DB_TEST_CONNECTION};

    my $connection = DBIx::Connection->new(
        name     => 'my_connection_name',
        dsn      => $ENV{DB_TEST_CONNECTION},
        username => $ENV{DB_TEST_USERNAME},
        password => $ENV{DB_TEST_PASSWORD},
    ); 
    
    my $table_not_exists = 0;
    my $error_handler = sub {
        $table_not_exists = 1;
        die;
    };
    $connection->set_custom_error_handler($error_handler);
    eval {
        $connection->record("SELECT * FROM test");
    };

SKIP: {
    
    if ($table_not_exists) {
        print "\n#missing test table CREATE TABLE test(id NUMBER, name VARCHAR2(100))\n";
        skip('missing table', 5);
    }
    
        $connection->do("DELETE FROM test");
        {    
            my $sql_handler = new DBIx::SQLHandler(
                name        => 'test_ins',
                connection  => $connection,
                sql         => "INSERT INTO test(id, name) VALUES(?, ?)"
            );
            is($sql_handler->execute(1, 'Smith'),1 ,'should insert a row');
        }
    
        {    
            my $sql_handler = new DBIx::SQLHandler(
                name        => 'test_ins',
                connection  => $connection,
                sql         => "UPDATE test SET name = ? WHERE id = ?"
            );
            is(int($sql_handler->execute('Smith1',0)), 0, 'should not update');
            is($sql_handler->execute('Smith1',1), 1, 'should update a row');
        }
    
    
        {    
            my $sql_handler = new DBIx::SQLHandler(
                name        => 'test_ins',
                connection  => $connection,
                sql         => "DELETE FROM test WHERE id = ?"
            );
            is(int($sql_handler->execute(0)), 0, 'should not update');
            is($sql_handler->execute(1), 1, 'should update a row');
        }
    }

}
