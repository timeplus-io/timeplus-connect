import datetime
from ipaddress import IPv4Address
from typing import Callable
from uuid import UUID

import pytest

from timeplus_connect.datatypes.format import set_write_format
from timeplus_connect.driver import Client


def type_available(test_client: Client, data_type: str):
    if not test_client.get_client_setting(f'allow_experimental_{data_type}_type'):
        pytest.skip(f'New {data_type.upper()} type not available in this version: {test_client.server_version}')


def test_variant(test_client: Client, table_context: Callable):
    type_available(test_client, 'variant')
    with table_context('basic_variants', [
        'key int32',
        'v1 variant(uint64, string, array(uint64), )',
        'v2 variant(ipv4, decimal(10, 2))']):
        data = [[1, 58322, None],
                [2, 'a string', 55.2],
                [3, 'bef56f14-0870-4f82-a35e-9a47eff45a5b', 777.25],
                [4, [120, 250], '243.12.55.44']
                ]
        test_client.insert('basic_variants', data)
        result = test_client.query('SELECT * except _tp_time FROM basic_variants ORDER BY key').result_set
        assert result[2][1] == UUID('bef56f14-0870-4f82-a35e-9a47eff45a5b')
        assert result[2][2] == 777.25
        assert result[3][1] == [120, 250]
        assert result[3][2] == IPv4Address('243.12.55.44')


def test_nested_variant(test_client: Client, table_context: Callable):
    type_available(test_client, 'variant')
    with table_context('nested_variants', [
        'key int32',
        'm1 map(string, variant(string, uint128, bool))',
        't1 tuple(int64, variant(bool, string, int32))',
        'a1 array(array(variant(string, datetime, float64)))',
    ]):
        data = [[1,
                 {'k1': 'string1', 'k2': 34782477743, 'k3':True},
                 (-40, True),
                 (('str3', 53.732),),
                 ],
                [2,
                 {'k1': False, 'k2': 's3872', 'k3': 100},
                 (340283, 'str'),
                 (),
                 ]
                ]
        test_client.insert('nested_variants', data)
        result = test_client.query('SELECT * except _tp_time FROM nested_variants ORDER BY key').result_set
        assert result[0][1]['k1'] == 'string1'
        assert result[0][1]['k2'] == 34782477743
        assert result[0][2] == (-40, True)
        assert result[0][3][0][1] == 53.732
        assert result[1][1]['k3'] == 100


def test_dynamic_nested(test_client: Client, table_context: Callable):
    type_available(test_client, 'dynamic')
    with table_context('nested_dynamics', [
        'm2 map(string, dynamic)'
        ], order_by='()'):
        data = [({'k4': 'string8', 'k5': 5000},)]
        test_client.insert('nested_dynamics', data)
        result = test_client.query('SELECT * except _tp_time FROM nested_dynamics').result_set
        assert result[0][0]['k5'] == '5000'


def test_dynamic(test_client: Client, table_context: Callable):
    type_available(test_client, 'dynamic')
    with table_context('basic_dynamic', [
        'key uint64',
        'v1 dynamic',
        'v2 dynamic']):
        data = [[1, 58322, 15.5],
                [3, 'bef56f14-0870-4f82-a35e-9a47eff45a5b', 777.25],
                [2, 'a string', 55.2],
                [4, [120, 250], 577.22]
                ]
        test_client.insert('basic_dynamic', data)
        result = test_client.query('SELECT * except _tp_time FROM basic_dynamic ORDER BY key').result_set
        assert result[2][1] == 'bef56f14-0870-4f82-a35e-9a47eff45a5b'
        assert result[3][1] == '[120, 250]'
        assert result[2][2] == '777.25'


def test_basic_json(test_client: Client, table_context: Callable):
    type_available(test_client, 'json')
    with table_context('new_json_basic', [
        'key int32',
        'value json',
        "null_value json"
    ]):
        jv3 = {'key3': 752, 'value.2': 'v2_rules', 'blank': None}
        jv1 = {'key1': 337, 'value.2': 'vvvv', 'HKD@spéçiäl': 'Special K', 'blank': 'not_really_blank'}
        njv2 = {'nk1': -302, 'nk2': {'sub1': 372, 'sub2': 'a string'}}
        njv3 = {'nk1': 5832.44, 'nk2': {'sub1': 47788382, 'sub2': 'sub2val', 'sub3': 'sub3str', 'space key': 'spacey'}}
        test_client.insert('new_json_basic', [
            [5, jv1, None],
            [20, None, njv2],
            [25, jv3, njv3]])

        result = test_client.query('SELECT * except _tp_time FROM new_json_basic ORDER BY key').result_set
        json1 = result[0][1]
        assert json1['HKD@spéçiäl'] == 'Special K'
        assert 'key3' not in json1
        json2 = result[1][2]
        assert json2['nk1'] == -302.0
        assert json2['nk2']['sub2'] == 'a string'
        assert json2['nk2'].get('sub3') is None
        json3 = result[2][1]
        assert json3['value']['2'] == 'v2_rules'
        assert 'blank' not in json3
        assert 'key1' not in json3
        assert json3['key3'] == 752
        null_json3 = result[2][2]
        assert null_json3['nk2']['space key'] == 'spacey'

        set_write_format('json', 'string')
        test_client.insert('new_json_basic', [[999, '{"key4": 283, "value.2": "str_value"}', '{"nk1":53}']])
        result = test_client.query('SELECT value.key4, null_value.nk1 FROM new_json_basic ORDER BY key').result_set
        assert result[3][0] == 283
        assert result[3][1] == 53


def test_typed_json(test_client: Client, table_context: Callable):
    type_available(test_client, 'json')
    with table_context('new_json_typed', [
        'key int32',
        'value json(max_dynamic_paths=150, `a.b` datetime64(3), SKIP a.c)'
    ]):
        v1 = '{"a":{"b":"2020-10-15T10:15:44.877", "c":"skip_me"}}'
        test_client.insert('new_json_typed', [[1, v1]])
        result = test_client.query('SELECT * except _tp_time FROM new_json_typed ORDER BY key')
        json1 = result.result_set[0][1]
        assert json1['a']['b'] == datetime.datetime(2020, 10, 15, 10, 15, 44, 877000)


def test_complex_json(test_client: Client, table_context: Callable):
    type_available(test_client, 'json')
    if not test_client.min_version('24.10'):
        pytest.skip('Complex JSON broken before 24.10')
    with table_context('new_json_complex', [
        'key int32',
        'value tuple(t json)'
        ]):
        data = [[100, ({'a': 'qwe123', 'b': 'main', 'c': None},)]]
        test_client.insert('new_json_complex', data)
        result = test_client.query('SELECT * except _tp_time FROM new_json_complex ORDER BY key')
        json1 = result.result_set[0][1]
        assert json1['t']['a'] == 'qwe123'


def test_json_str_time(test_client: Client):
    if not test_client.min_version('2.9'):
        pytest.skip('JSON string/numbers bug before 2.9, skipping')
    result = test_client.query("SELECT '{\"timerange\": \"2025-01-01T00:00:00+0000\"}'::json").result_set
    assert result[0][0]['timerange'] == datetime.datetime(2025, 1, 1)

    # The following query is broken -- looks like something to do with Nullable(String) in the Tuple
    # result = test_client.query("SELECT'{\"k\": [123, \"xyz\"]}'::json",
    #                           settings={'input_format_json_read_numbers_as_strings': 0}).result_set
