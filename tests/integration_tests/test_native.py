import decimal
import os
import uuid
from datetime import datetime, date
from ipaddress import IPv4Address, IPv6Address
from typing import Callable
from time import sleep
import pytest

from clickhouse_connect.datatypes.format import set_default_formats, clear_default_format, set_read_format, \
    set_write_format
from clickhouse_connect.driver import Client
from clickhouse_connect.driver.common import coerce_bool


def test_low_card(test_client: Client, table_context: Callable):
    with table_context('native_test', ['key low_cardinality(int32)', 'value_1 low_cardinality(string)']):
        test_client.insert('native_test', [[55, 'TV1'], [-578328, 'TV38882'], [57372, 'Kabc/defXX']], ['key', 'value_1'])
        sleep(3)
        result = test_client.query("SELECT * except _tp_time FROM table(native_test) WHERE value_1 LIKE '%abc/def%'")
        assert len(result.result_set) == 1


def test_low_card_uuid(test_client: Client, table_context: Callable):
    with table_context('low_card_uuid', ['dt Date', 'low_card_uuid low_cardinality(uuid)']):
        data = ([date(2023, 1, 1), '80397B00E0B248AFAF34AE11A5546A3B'],
               [date(2024, 1, 1), '70397B00-E0B2-48AF-AF34-AE11A5546A3B'])
        test_client.insert('low_card_uuid', data, ['dt', 'low_card_uuid'])
        result = test_client.query(
            "SELECT * except _tp_time FROM low_card_uuid WHERE _tp_time > earliest_ts() ORDER BY dt LIMIT 2"
        ).result_set
        assert len(result) == 2
        assert str(result[0][1]) == '80397b00-e0b2-48af-af34-ae11a5546a3b'
        assert str(result[1][1]) == '70397b00-e0b2-48af-af34-ae11a5546a3b'


def test_bare_datetime64(test_client: Client, table_context: Callable):
    with table_context('bare_datetime64_test', ['key uint32', 'dt64 DateTime64']):
        test_client.insert('bare_datetime64_test',
                           [[1, datetime(2023, 3, 25, 10, 5, 44, 772402)],
                            [2, datetime.now()],
                            [3, datetime(1965, 10, 15, 12, 0, 0)]],
                            ['key', 'dt64'])
        result = test_client.query(
            'SELECT * except _tp_time FROM bare_datetime64_test WHERE _tp_time > earliest_ts() ORDER BY key LIMIT 3'
        ).result_rows
        assert result[0][0] == 1
        assert result[0][1] == datetime(2023, 3, 25, 10, 5, 44, 772000)
        assert result[2][1] == datetime(1965, 10, 15, 12, 0, 0)


def test_nulls(test_client: Client, table_context: Callable):
    with table_context('nullable_test', ['key uint32', 'null_str nullable(string)', 'null_int nullable(int64)']):
        test_client.insert('nullable_test', [[1, None, None],
                                             [2, 'nonnull', -57382882345666],
                                             [3, None, 5882374747732834],
                                             [4, 'nonnull2', None]],
                                             ['key', 'null_str', 'null_int'])
        result = test_client.query(
            'SELECT * except _tp_time FROM nullable_test WHERE _tp_time > earliest_ts() ORDER BY key LIMIT 4',
            use_none=False
        ).result_rows
        assert result[2] == (3, '', 5882374747732834)
        assert result[3] == (4, 'nonnull2', 0)
        result = test_client.query(
            'SELECT * except _tp_time FROM nullable_test WHERE _tp_time > earliest_ts() ORDER BY key LIMIT 4'
        ).result_rows
        assert result[1] == (2, 'nonnull', -57382882345666)
        assert result[2] == (3, None, 5882374747732834)
        assert result[3] == (4, 'nonnull2', None)


def test_old_json(test_client: Client, table_context: Callable):
    if not coerce_bool(os.environ.get('CLICKHOUSE_CONNECT_TEST_OLD_JSON_TYPE')):
        pytest.skip('Deprecated JSON type not tested')
    with table_context('old_json_test', [
        'key int32',
        'value JSON',
        'e2 int32',
        "null_value Object(nullable('json'))"
    ]):
        jv1 = {'key1': 337, 'value.2': 'vvvv', 'HKD@spéçiäl': 'Special K', 'blank': 'not_really_blank'}
        jv3 = {'key3': 752, 'value.2': 'v2_rules', 'blank': None}
        njv2 = {'nk1': -302, 'nk2': {'sub1': 372, 'sub2': 'a string'}}
        njv3 = {'nk1': 5832.44, 'nk2': {'sub1': 47788382, 'sub2':'sub2val', 'sub3': 'sub3str', 'space key': 'spacey'}}
        test_client.insert('old_json_test', [
            [5, jv1, -44, None],
            [20, None, 5200, njv2],
            [25, jv3, 7302, njv3]],
            ['key', 'value', 'e2', 'null_value'])

        result = test_client.query('SELECT * except _tp_time FROM old_json_test WHERE _tp_time > earliest_ts() ORDER BY key LIMIT 3')
        json1 = result.result_set[0][1]
        assert json1['HKD@spéçiäl'] == 'Special K'
        assert json1['key3'] == 0
        json2 = result.result_set[1][3]
        assert json2['nk1'] == -302.0
        assert json2['nk2']['sub2'] == 'a string'
        assert json2['nk2']['sub3'] is None
        json3 = result.result_set[2][1]
        assert json3['value.2'] == 'v2_rules'
        assert json3['blank'] == ''
        assert json3['key1'] == 0
        assert json3['key3'] == 752
        null_json3 = result.result_set[2][3]
        assert null_json3['nk2']['space key'] == 'spacey'

        set_write_format('JSON', 'string')
        test_client.insert('native_json_test',
                           [[999, '{"key4": 283, "value.2": "str_value"}', 77, '{"nk1":53}']],
                           ['key', 'value', 'e2', 'null_value'])
        result = test_client.query(
            'SELECT value.key4, null_value.nk1 FROM native_json_test WHERE _tp_time > earliest_ts() ORDER BY key LIMIT 4'
        )
        assert result.result_set[3][0] == 283
        assert result.result_set[3][1] == 53


def test_read_formats(test_client: Client):
    test_client.command('DROP STREAM IF EXISTS read_format_test')
    test_client.command('CREATE STREAM read_format_test (key int32, id uuid, fs fixed_string(10), v4 ipv4,' +
                        'ip_array array(ipv6), tup tuple(u1 uint64, ip2 ipv4))')
    uuid1 = uuid.UUID('23E45688e89B-12D3-3273-426614174000')
    uuid2 = uuid.UUID('77AA3278-3728-12d3-5372-000377723832')
    row1 = (1, uuid1, '530055777k', '10.251.30.50', ['2600::', '2001:4860:4860::8844'], (7372, '10.20.30.203'))
    row2 = (2, uuid2, 'short str', '10.44.75.20', ['74:382::3332', '8700:5200::5782:3992'], (7320, '252.18.4.50'))
    column_names = ['key', 'id', 'fs', 'v4', 'ip_array', 'tup']
    test_client.insert('read_format_test', [row1, row2], column_names)

    result = test_client.query('SELECT * except _tp_time FROM read_format_test WHERE _tp_time > earliest_ts() LIMIT 2').result_set
    assert result[0][1] == uuid1
    assert result[1][3] == IPv4Address('10.44.75.20')
    assert result[0][2] == b'\x35\x33\x30\x30\x35\x35\x37\x37\x37\x6b'
    assert result[0][5]['u1'] == 7372
    assert result[0][5]['ip2'] == IPv4Address('10.20.30.203')

    set_default_formats('uuid', 'string', 'ip*', 'string', 'fixed_string', 'string')
    result = test_client.query('SELECT * except _tp_time FROM read_format_test WHERE _tp_time > earliest_ts() LIMIT 2').result_set
    assert result[0][1] == '23e45688-e89b-12d3-3273-426614174000'
    assert result[1][3] == '10.44.75.20'
    assert result[0][2] == '530055777k'
    assert result[0][4][1] == '2001:4860:4860::8844'

    clear_default_format('ip*')
    result = test_client.query('SELECT * except _tp_time FROM read_format_test WHERE _tp_time > earliest_ts() LIMIT 2').result_set
    assert result[0][1] == '23e45688-e89b-12d3-3273-426614174000'
    assert result[1][3] == IPv4Address('10.44.75.20')
    assert result[0][4][1] == IPv6Address('2001:4860:4860::8844')
    assert result[0][2] == '530055777k'

    # Test query formats
    result = test_client.query(
        'SELECT * except _tp_time FROM read_format_test WHERE _tp_time > earliest_ts() LIMIT 2',
        query_formats={'IP*': 'string', 'tup': 'json'}).result_set
    assert result[1][3] == '10.44.75.20'
    assert result[0][5] == b'{"u1":7372,"ip2":"10.20.30.203"}'

    # Ensure that the query format clears
    result = test_client.query(
        'SELECT * except _tp_time FROM read_format_test WHERE _tp_time > earliest_ts() LIMIT 2'
    ).result_set
    assert result[1][3] == IPv4Address('10.44.75.20')
    assert result[0][5]['ip2'] == IPv4Address('10.20.30.203')

    # Test column formats
    result = test_client.query(
        'SELECT * except _tp_time FROM read_format_test WHERE _tp_time > earliest_ts() LIMIT 2',
        column_formats={'v4': 'string', 'tup': 'tuple'}
    ).result_set
    assert result[1][3] == '10.44.75.20'
    assert result[0][5][1] == IPv4Address('10.20.30.203')

    # Ensure that the column format clears
    result = test_client.query(
        'SELECT * except _tp_time FROM read_format_test WHERE _tp_time > earliest_ts() LIMIT 2'
    ).result_set
    assert result[1][3] == IPv4Address('10.44.75.20')
    assert result[0][5]['ip2'] == IPv4Address('10.20.30.203')

    # Test sub column formats
    set_read_format('tuple', 'tuple')
    result = test_client.query('SELECT * except _tp_time FROM read_format_test WHERE _tp_time > earliest_ts() LIMIT 2',
                               column_formats={'tup': {'ip*': 'string'}}).result_set
    assert result[0][5][1] == '10.20.30.203'

    set_read_format('tuple', 'native')
    result = test_client.query('SELECT * except _tp_time FROM read_format_test WHERE _tp_time > earliest_ts() LIMIT 2',
                               column_formats={'tup': {'ip*': 'string'}}).result_set
    assert result[0][5]['ip2'] == '10.20.30.203'


def test_tuple_inserts(test_client: Client, table_context: Callable):
    with table_context('insert_tuple_test', ['key int32', 'named tuple(fl float64, `ns space` nullable(string))',
                                             'unnamed tuple(float64, nullable(string))']):
        data = [[1, (3.55, 'str1'), (555, None)], [2, (-43.2, None), (0, 'str2')]]
        column_names = ['key', 'named', 'unnamed']
        test_client.insert('insert_tuple_test', data, column_names, settings={'insert_deduplication_token': 5772})

        data = [[1, {'fl': 3.55, 'ns space': 'str1'}, (555, None)], [2, {'fl': -43.2}, (0, 'str2')]]
        test_client.insert('insert_tuple_test', data, column_names, settings={'insert_deduplication_token': 5773})
        sleep(3)
        query_result = test_client.query('SELECT * except _tp_time FROM table(insert_tuple_test)').result_rows
        assert len(query_result) == 4
        assert query_result[0] == query_result[1]
        assert query_result[2] == query_result[3]


def test_agg_function(test_client: Client, table_context: Callable):
    with table_context('agg_func_test', ['key int32',
                                         'str simple_aggregate_function(any, string)',
                                         'lc_str simple_aggregate_function(any, low_cardinality(string))']):
        test_client.insert('agg_func_test', [(1, 'str', 'lc_str')], ['key', 'str', 'lc_str'])
        sleep(3)
        row = test_client.query('SELECT str, lc_str FROM table(agg_func_test)').first_row
        assert row[0] == 'str'
        assert row[1] == 'lc_str'


def test_decimal_rounding(test_client: Client, table_context: Callable):
    test_vals = [732.4, 75.57, 75.49, 40.16]
    with table_context('test_decimal', ['key int32, value Decimal(10, 2)']):
        test_client.insert('test_decimal',
                           [[ix, x] for ix, x in enumerate(test_vals)],
                           ['key', 'value'])
        values = test_client.query('SELECT value FROM test_decimal WHERE _tp_time > earliest_ts() LIMIT 4').result_columns[0]
    with decimal.localcontext() as dec_ctx:
        dec_ctx.prec = 10
        assert [decimal.Decimal(str(x)) for x in test_vals] == values


def test_empty_maps(test_client: Client):
    result = test_client.query("select Cast(([],[]), 'map(string, map(string, string))')")
    assert result.first_row[0] == {}


def test_fixed_str_padding(test_client: Client, table_context: Callable):
    table = 'test_fixed_str_padding'
    with table_context(table, 'key int32, value fixed_string(3)'):
        column_names = ['key', 'value']
        test_client.insert(table, [[1, 'abc']], column_names)
        test_client.insert(table, [[2, 'a']], column_names)
        test_client.insert(table, [[3, '']], column_names)
        result = test_client.query(f'select * from {table} WHERE _tp_time > earliest_ts() ORDER BY key LIMIT 3')
        assert result.result_columns[1] == [b'abc', b'a\x00\x00', b'\x00\x00\x00']


def test_nonstandard_column_names(test_client: Client, table_context: Callable):
    table = 'пример_кириллица'
    with table_context(table, 'колонка string') as t:
        test_client.insert(t.table, (('привет',),), ['колонка'])
        result = test_client.query(f'SELECT * except _tp_time FROM {t.table} WHERE _tp_time > earliest_ts() LIMIT 1').result_set
        assert result[0][0] == 'привет'
