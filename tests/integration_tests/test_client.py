from pathlib import Path
from time import sleep
from typing import Callable

import pytest

from timeplus_connect import create_client
from timeplus_connect.driver.client import Client
from timeplus_connect.driver.exceptions import DatabaseError
from tests.integration_tests.conftest import TestConfig

CSV_CONTENT = """abc,1,1
abc,1,0
def,1,0
hij,1,1
hij,1,
klm,1,0
klm,1,"""


def test_ping(test_client: Client):
    assert test_client.ping() is True


def test_query(test_client: Client):
    result = test_client.query('SELECT * except _tp_time FROM numbers(10)')
    print(result.result_set)
    assert len(result.result_set) > 0
    assert result.row_count > 0
    assert result.first_item == next(result.named_results())


def test_command(test_client: Client):
    version = test_client.command('SELECT version()')
    assert int(version.split('.')[0]) >= 1


def test_client_name(test_client: Client):
    user_agent = test_client.headers['User-Agent']
    assert 'test' in user_agent
    assert 'py/' in user_agent


# def test_transport_settings(test_client: Client):
#     result = test_client.query('SELECT name,database FROM system.tables',
#                                transport_settings={'X-Workload': 'ONLINE'})
#     assert result.column_names == ('name', 'database')
#     assert len(result.result_set) > 0


def test_none_database(test_client: Client):
    old_db = test_client.database
    test_db = test_client.command('select current_database()')
    assert test_db == old_db
    try:
        test_client.database = None
        test_client.query('SELECT * except _tp_time FROM system.tables')
        test_db = test_client.command('select current_database()')
        assert test_db == 'default'
        test_client.database = old_db
        test_db = test_client.command('select current_database()')
        assert test_db == old_db
    finally:
        test_client.database = old_db


def test_session_params(test_config: TestConfig):
    session_id = 'TEST_SESSION_ID_' + test_config.test_database
    client = create_client(
        session_id=session_id,
        host=test_config.host,
        port=test_config.port,
        username=test_config.username,
        password=test_config.password)
    result = client.query('SELECT number FROM system.numbers LIMIT 5',
                          settings={'query_id': 'test_session_params'}).result_set
    assert len(result) == 5

    if client.min_version('21'):
        if test_config.host != 'localhost':
            return  # By default, the session log isn't enabled, so we only validate in environments we control
        sleep(10)  # Allow the log entries to flush to tables
        result = client.query(
            f"SELECT session_id, user FROM system.session_log WHERE session_id = '{session_id}' AND " +
            'event_time > now() - 30').result_set
        assert result[0] == (session_id, test_config.username)
        result = client.query(
            "SELECT query_id, user FROM system.query_log WHERE query_id = 'test_session_params' AND " +
            'event_time > now() - 30').result_set
        assert result[0] == ('test_session_params', test_config.username)


def test_dsn_config(test_config: TestConfig):
    session_id = 'TEST_DSN_SESSION_' + test_config.test_database
    dsn = (f'timeplus://{test_config.username}:{test_config.password}@{test_config.host}:{test_config.port}' +
           f'/{test_config.test_database}?session_id={session_id}&show_clickhouse_errors=false')
    client = create_client(dsn=dsn)
    assert client.get_client_setting('session_id') == session_id
    count = client.command('SELECT count() from numbers(10)')
    assert client.database is None
    assert count > 0
    try:
        client.query('SELECT nothing')
    except DatabaseError as ex:
        assert 'returned an error' in str(ex)
    client.close()


def test_get_columns_only(test_client: Client):
    result = test_client.query('SELECT name, database FROM system.tables LIMIT 0')
    assert result.column_names == ('name', 'database')
    assert len(result.result_set) == 0

    test_client.query('CREATE STREAM IF NOT EXISTS test_zero_insert (v int8) ENGINE MergeTree() ORDER BY v')
    test_client.query('INSERT INTO test_zero_insert SELECT 1 LIMIT 0')


def test_no_limit(test_client: Client):
    old_limit = test_client.query_limit
    test_client.limit = 0
    result = test_client.query('SELECT name FROM system.databases')
    assert len(result.result_set) > 0
    test_client.limit = old_limit


def test_multiline_query(test_client: Client):
    result = test_client.query("""
    SELECT * except _tp_time
    FROM system.tables
    """)
    assert len(result.result_set) > 0


def test_query_with_inline_comment(test_client: Client):
    result = test_client.query("""
    SELECT * except _tp_time
    -- This is just a comment
    FROM system.tables LIMIT 77
    -- A second comment
    """)
    assert len(result.result_set) > 0


def test_query_with_comment(test_client: Client):
    result = test_client.query("""
    SELECT * except _tp_time
    /* This is:
    a multiline comment */
    FROM system.tables
    """)
    assert len(result.result_set) > 0


def test_insert_csv_format(test_client: Client, test_table_engine: str):
    test_client.command('DROP STREAM IF EXISTS test_csv')
    test_client.command(
        'CREATE STREAM test_csv ("key" string, "val1" int32, "val2" int32) ' +
        f'ENGINE {test_table_engine}(1, 1, rand())')
    sql = f'INSERT INTO test_csv ("key", "val1", "val2") FORMAT CSV {CSV_CONTENT}'
    test_client.command(sql)
    result = test_client.query('SELECT * except _tp_time from test_csv WHERE _tp_time > earliest_ts() LIMIT 7')

    def compare_rows(row_1, row_2):
        return all(c1 == c2 for c1, c2 in zip(row_1, row_2))

    assert len(result.result_set) == 7
    assert compare_rows(result.result_set[0], ['abc', 1, 1])
    assert compare_rows(result.result_set[4], ['hij', 1, 0])


def test_non_latin_query(test_client: Client):
    result = test_client.query("SELECT database, name FROM system.tables WHERE engine_full IN ('空')")
    assert len(result.result_set) == 0


def test_error_decode(test_client: Client):
    try:
        test_client.query("SELECT database, name FROM system.tables WHERE has_own_data = '空'")
    except DatabaseError as ex:
        assert '空' in str(ex)


def test_command_as_query(test_client: Client):
    # Test that non-SELECT and non-INSERT statements are treated as commands and
    # just return the QueryResult metadata
    result = test_client.query("SET count_distinct_implementation = 'uniq'")
    assert 'query_id' in result.first_item


def test_show_create(test_client: Client):
    # if not test_client.min_version('21'):
    #     pytest.skip(f'Not supported server version {test_client.server_version}')
    test_client.command('DROP STREAM IF EXISTS dummy')
    test_client.command('CREATE STREAM dummy (i int, s string)')
    result = test_client.query('SHOW CREATE dummy')
    result.close()
    assert 'statement' in result.column_names


def test_empty_result(test_client: Client):
    assert len(test_client.query("SELECT * except _tp_time FROM system.tables WHERE name = '_NOT_A THING'").result_rows) == 0


def test_temporary_tables(test_client: Client):
    test_client.command("""
    CREATE STREAM temp_test_table
            (
                field1 string,
                field2 string
            )""")

    test_client.command ("INSERT INTO temp_test_table (field1, field2) VALUES ('test1', 'test2'), ('test3', 'test4')")
    df = test_client.query_df('SELECT * except _tp_time FROM temp_test_table WHERE _tp_time > earliest_ts() LIMIT 2')
    test_client.insert_df('temp_test_table', df)
    df = test_client.query_df('SELECT * except _tp_time FROM temp_test_table WHERE _tp_time > earliest_ts() LIMIT 4')
    assert len(df['field1']) == 4
    test_client.command('DROP STREAM temp_test_table')


def test_str_as_bytes(test_client: Client, table_context: Callable):
    with table_context('test_insert_bytes', ['key uint32', 'byte_str string', 'n_byte_str nullable(string)']):
        col_names = ['key', 'byte_str', 'n_byte_str']
        test_client.insert('test_insert_bytes', [[0, 'str_0', 'n_str_0'], [1, 'str_1', 'n_str_0']], col_names)
        test_client.insert('test_insert_bytes', [[2, 'str_2'.encode('ascii'), 'n_str_2'.encode()],
                                                 [3, b'str_3', b'str_3'],
                                                 [4, bytearray([5, 120, 24]), bytes([16, 48, 52])],
                                                 [5, b'', None]
                                                 ], col_names)
        result_set = test_client.query(
            'SELECT * except _tp_time FROM test_insert_bytes WHERE _tp_time > earliest_ts() LIMIT 6'
        ).result_columns
        assert result_set[1][0] == 'str_0'
        assert result_set[1][3] == 'str_3'
        assert result_set[2][5] is None
        assert result_set[1][4].encode() == b'\x05\x78\x18'
        result_set = test_client.query('SELECT * except _tp_time FROM test_insert_bytes WHERE _tp_time > earliest_ts() LIMIT 6',
                                       query_formats={'string': 'bytes'}).result_columns
        assert result_set[1][0] == b'str_0'
        assert result_set[1][4] == b'\x05\x78\x18'
        assert result_set[2][4] == b'\x10\x30\x34'


@pytest.mark.skip("DB::Exception: Unknown function format.")
def test_embedded_binary(test_client: Client):
    binary_params = {'$xx$': 'col1,col2\n100,700'.encode()}
    result = test_client.raw_query(
        'SELECT col2, col1 FROM format(CSVWithNames, $xx$)', parameters=binary_params)
    assert result == b'700\t100\n'

    movies_file = f'{Path(__file__).parent}/movies.parquet'
    with open(movies_file, 'rb') as f:  # read bytes
        data = f.read()
    binary_params = {'$parquet$': data}
    result = test_client.query(
        'SELECT movie, rating FROM format(Parquet, $parquet$) ORDER BY movie', parameters=binary_params)
    assert result.first_item['movie'] == '12 Angry Men'

    binary_params = {'$mult$': 'foobar'.encode()}
    result = test_client.query("SELECT $mult$ as m1, $mult$ as m2 WHERE m1 = 'foobar'", parameters=binary_params)
    assert result.first_item['m2'] == 'foobar'
