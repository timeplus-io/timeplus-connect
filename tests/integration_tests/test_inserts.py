from decimal import Decimal
from typing import Callable
from time import sleep

from timeplus_connect.driver.client import Client
from timeplus_connect.driver.exceptions import DataError

def test_insert(test_client: Client):
    if test_client.min_version('19'):
        test_client.command('DROP STREAM IF EXISTS test_system_insert')
    else:
        test_client.command('DROP STREAM IF EXISTS test_system_insert SYNC')
    test_client.command('CREATE STREAM test_system_insert AS system.tables')
    tables_result = test_client.query('SELECT * except _tp_time from system.tables')
    data = tables_result.result_set
    for i, _ in enumerate(data):
        data[i] = data[i] + (0, )
    test_client.insert(table='test_system_insert', column_names='*', data=data)
    sleep(3)
    copy_result = test_client.command('SELECT count() from table(test_system_insert)')
    assert tables_result.row_count == copy_result
    test_client.command('DROP STREAM IF EXISTS test_system_insert')

def test_decimal_conv(test_client: Client, table_context: Callable):
    with table_context('test_num_conv', ['col1 uint64', 'col2 int32', 'f1 float64']):
        data = [[Decimal(5), Decimal(-182), Decimal(55.2)], [Decimal(57238478234), Decimal(77), Decimal(-29.5773)]]
        column_names = ['col1', 'col2', 'f1']
        test_client.insert('test_num_conv', data, column_names)
        result = test_client.query('SELECT * except _tp_time FROM test_num_conv WHERE _tp_time > earliest_ts() LIMIT 2').result_set
        assert result == [(5, -182, 55.2), (57238478234, 77, -29.5773)]


def test_float_decimal_conv(test_client: Client, table_context: Callable):
    with table_context('test_float_to_dec_conv', ['col1 Decimal32(6)','col2 Decimal32(6)', 'col3 Decimal128(6)', 'col4 Decimal128(6)']):
        data = [[0.492917, 0.49291700, 0.492917, 0.49291700]]
        column_names = ['col1', 'col2', 'col3', 'col4']
        test_client.insert('test_float_to_dec_conv', data, column_names)
        result = test_client.query('SELECT * except _tp_time FROM test_float_to_dec_conv WHERE _tp_time > earliest_ts() LIMIT 1').result_set
        assert result == [(Decimal("0.492917"), Decimal("0.492917"), Decimal("0.492917"), Decimal("0.492917"))]


def test_bad_data_insert(test_client: Client, table_context: Callable):
    with table_context('test_bad_insert', ['key int32', 'float_col float64']):
        data = [[1, 3.22], [2, 'nope']]
        column_names = ['key', 'float_col']
        try:
            test_client.insert('test_bad_insert', data, column_names)
        except DataError as ex:
            assert 'array' in str(ex)


def test_bad_strings(test_client: Client, table_context: Callable):
    with table_context('test_bad_strings', 'key int32, fs fixed_string(6), nsf nullable(fixed_string(4))'):
        try:
            test_client.insert('test_bad_strings', [[1, b'\x0535', None]], ['key', 'fs', 'nsf'])
        except DataError as ex:
            assert 'match' in str(ex)
        try:
            test_client.insert('test_bad_strings', [[1, b'\x0535abc', '😀🙃']], ['key', 'fs', 'nsf'])
        except DataError as ex:
            assert 'encoded' in str(ex)


def test_low_card_dictionary_size(test_client: Client, table_context: Callable):
    with table_context('test_low_card_dict', 'key int32, lc low_cardinality(string)',
                       settings={'index_granularity': 65536 }):
        data = [[x, str(x)] for x in range(30000)]
        column_names = ['key', 'lc']
        test_client.insert('test_low_card_dict', data, column_names)
        sleep(3)
        assert 30000 == test_client.command('SELECT count() FROM table(test_low_card_dict)')


def test_column_names_spaces(test_client: Client, table_context: Callable):
    with table_context('test_column_spaces',
                       columns=['key 1', 'value 1'],
                       column_types=['int32', 'string']):
        data = [[1, 'str 1'], [2, 'str 2']]
        column_names = ['key 1', 'value 1']
        test_client.insert('test_column_spaces', data, column_names)
        result = test_client.query('SELECT * except _tp_time FROM test_column_spaces WHERE _tp_time > earliest_ts() LIMIT 2').result_rows
        assert result[0][0] == 1
        assert result[1][1] == 'str 2'


def test_numeric_conversion(test_client: Client, table_context: Callable):
    with table_context('test_numeric_convert',
                       columns=['key int32', 'n_int nullable(uint64)', 'n_flt nullable(float64)']):
        data = [[1, None, None], [2, '2', '5.32']]
        column_names = ['key', 'n_int', 'n_flt']
        test_client.insert('test_numeric_convert', data, column_names)
        sleep(3)
        result = test_client.query('SELECT * FROM table(test_numeric_convert)').result_rows
        assert result[1][1] == 2
        assert result[1][2] == float('5.32')
        test_client.command('TRUNCATE STREAM test_numeric_convert')
        data = [[0, '55', '532.48'], [1, None, None], [2, '2', '5.32']]
        test_client.insert('test_numeric_convert', data, column_names)
        sleep(3)
        result = test_client.query('SELECT * FROM table(test_numeric_convert)').result_rows
        assert result[0][1] == 55
        assert result[0][2] == 532.48
        assert result[1][1] is None
        assert result[2][1] == 2
        assert result[2][2] == 5.32
