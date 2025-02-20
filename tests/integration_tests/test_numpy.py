import datetime
import logging
import os
import random
from typing import Callable
import pytest
from timeplus_connect.driver.exceptions import ProgrammingError

from timeplus_connect.driver import Client
from timeplus_connect.driver.options import np
from tests.helpers import list_equal, random_query
from tests.integration_tests.datasets import basic_ds, basic_ds_columns, basic_ds_types, basic_ds_types_ver19, \
    null_ds, null_ds_columns, null_ds_types, dt_ds, dt_ds_columns, dt_ds_types


logger = logging.getLogger(__name__)
pytestmark = pytest.mark.skipif(np is None, reason='Numpy package not installed')


def test_numpy_dates(test_client: Client, table_context: Callable):
    np_array = np.array(dt_ds, dtype='datetime64[s]').reshape(-1, 1)
    source_arr = np_array.copy()
    with table_context('test_numpy_dates', dt_ds_columns, dt_ds_types):
        test_client.insert('test_numpy_dates', np_array, dt_ds_columns)
        new_np_array = test_client.query_np('SELECT * except _tp_time FROM test_numpy_dates WHERE _tp_time > earliest_ts() LIMIT 2')
        assert np.array_equal(np_array, new_np_array)
        assert np.array_equal(source_arr, np_array)


def test_invalid_date(test_client):
    try:
        sql = "SELECT cast(now64(), 'DateTime64(1)')"
        if not test_client.min_version('20'):
            sql = "SELECT cast(now(), 'DateTime')"
        test_client.query_df(sql)
    except ProgrammingError as ex:
        assert 'milliseconds' in str(ex)


def test_numpy_record_type(test_client: Client, table_context: Callable):
    dt_type = 'datetime64[ns]'
    ds_types = basic_ds_types
    if not test_client.min_version('20'):
        dt_type = 'datetime64[s]'
        ds_types = basic_ds_types_ver19

    np_array = np.array(basic_ds, dtype=f'U20,int32,float,U20,{dt_type},U20')
    source_arr = np_array.copy()
    np_array.dtype.names = basic_ds_columns
    with table_context('test_numpy_basic', basic_ds_columns, ds_types):
        test_client.insert('test_numpy_basic', np_array, basic_ds_columns)
        new_np_array = test_client.query_np(
            'SELECT * except _tp_time FROM test_numpy_basic WHERE _tp_time > earliest_ts() LIMIT 3',
            max_str_len=20
        )
        assert np.array_equal(np_array, new_np_array)
        empty_np_array = test_client.query_np("SELECT * except _tp_time FROM table(test_numpy_basic) WHERE key = 'NOT A KEY'")
        assert len(empty_np_array) == 0
        assert np.array_equal(source_arr, np_array)


def test_numpy_object_type(test_client: Client, table_context: Callable):
    dt_type = 'datetime64[ns]'
    ds_types = basic_ds_types
    if not test_client.min_version('20'):
        dt_type = 'datetime64[s]'
        ds_types = basic_ds_types_ver19

    np_array = np.array(basic_ds, dtype=f'O,int32,float,O,{dt_type},O')
    np_array.dtype.names = basic_ds_columns
    source_arr = np_array.copy()
    with table_context('test_numpy_basic', basic_ds_columns, ds_types):
        test_client.insert('test_numpy_basic', np_array, basic_ds_columns)
        new_np_array = test_client.query_np('SELECT * except _tp_time FROM test_numpy_basic WHERE _tp_time > earliest_ts() LIMIT 3')
        assert np.array_equal(np_array, new_np_array)
        assert np.array_equal(source_arr, np_array)


def test_numpy_nulls(test_client: Client, table_context: Callable):
    np_types = [(col_name, 'O') for col_name in null_ds_columns]
    np_array = np.rec.fromrecords(null_ds, dtype=np_types)
    source_arr = np_array.copy()
    with table_context('test_numpy_nulls', null_ds_columns, null_ds_types):
        test_client.insert('test_numpy_nulls', np_array, null_ds_columns)
        new_np_array = test_client.query_np(
            'SELECT * except _tp_time FROM test_numpy_nulls WHERE _tp_time > earliest_ts() LIMIT 4',
            use_none=True
        )
        assert list_equal(np_array.tolist(), new_np_array.tolist())
        assert list_equal(source_arr.tolist(), np_array.tolist())


def test_numpy_matrix(test_client: Client, table_context: Callable):
    source = [25000, -37283, 4000, 25770, 40032, 33002, 73086, -403882, 57723, 77382,
              1213477, 2, 0, 5777732, 99827616]
    source_array = np.array(source, dtype='int32')
    matrix = source_array.reshape((5, 3))
    matrix_copy = matrix.copy()
    with table_context('test_numpy_matrix', ['col1 int32', 'col2 int32', 'col3 int32']):
        test_client.insert('test_numpy_matrix', matrix, ['col1', 'col2', 'col3'])
        py_result = test_client.query(
            'SELECT * except _tp_time FROM test_numpy_matrix WHERE _tp_time > earliest_ts() ORDER BY col1 LIMIT 5'
        ).result_set
        assert list(py_result[1]) == [25000, -37283, 4000]
        numpy_result = test_client.query_np(
            'SELECT * except _tp_time FROM test_numpy_matrix WHERE _tp_time > earliest_ts() ORDER BY col1 LIMIT 5'
        )
        assert list(numpy_result[1]) == list(py_result[1])
        test_client.command('TRUNCATE test_numpy_matrix')
        numpy_result = test_client.query_np('SELECT * except _tp_time FROM table(test_numpy_matrix)')
        assert np.size(numpy_result) == 0
        assert np.array_equal(matrix, matrix_copy)


def test_numpy_bigint_matrix(test_client: Client, table_context: Callable):
    source = [25000, -37283, 4000, 25770, 40032, 33002, 73086, -403882, 57723, 77382,
              1213477, 2, 0, 5777732, 99827616]
    source_array = np.array(source, dtype='int64')
    matrix = source_array.reshape((5, 3))
    matrix_copy = matrix.copy()
    columns = ['col1 uint256', 'col2 int64', 'col3 int128']
    # if not test_client.min_version('21'):
    #     columns = ['col1 uint64', 'col2 int64', 'col3 int64']
    with table_context('test_numpy_bigint_matrix', columns):
        test_client.insert('test_numpy_bigint_matrix', matrix, ['col1', 'col2', 'col3'])
        py_result = test_client.query(
            'SELECT * except _tp_time FROM test_numpy_bigint_matrix WHERE _tp_time > earliest_ts() ORDER BY col1  LIMIT 5'
        ).result_set
        assert list(py_result[1]) == [25000, -37283, 4000]
        numpy_result = test_client.query_np(
            'SELECT * except _tp_time FROM test_numpy_bigint_matrix WHERE _tp_time > earliest_ts() ORDER BY col1 LIMIT 5'
        )
        assert list(numpy_result[1]) == list(py_result[1])
        assert np.array_equal(matrix, matrix_copy)


def test_numpy_bigint_object(test_client: Client, table_context: Callable):
    source = [('key1', 347288, datetime.datetime(1999, 10, 15, 12, 3, 44)),
              ('key2', '348147832478', datetime.datetime.now())]
    np_array = np.array(source, dtype='O,uint64,datetime64[s]')
    source_arr = np_array.copy()
    columns = ['key string', 'big_value uint256', 'dt DateTime']
    if not test_client.min_version('21'):
        columns = ['key string', 'big_value uint64', 'dt DateTime']
    with table_context('test_numpy_bigint_object', columns):
        test_client.insert('test_numpy_bigint_object', np_array, ['key', 'big_value', 'dt'])
        py_result = test_client.query(
            'SELECT * except _tp_time FROM test_numpy_bigint_object WHERE _tp_time > earliest_ts() LIMIT 2'
        ).result_set
        assert list(py_result[0]) == list(source[0])
        numpy_result = test_client.query_np(
            'SELECT * except _tp_time FROM test_numpy_bigint_object WHERE _tp_time > earliest_ts() LIMIT 2'
        )
        assert list(py_result[1]) == list(numpy_result[1])
        assert np.array_equal(source_arr, np_array)


def test_numpy_streams(test_client: Client):
    if not test_client.min_version('22'):
        pytest.skip(f'generateRandom is not supported in this server version {test_client.server_version}')
    runs = os.environ.get('CLICKHOUSE_CONNECT_TEST_FUZZ', '250')
    for _ in range(int(runs) // 2):
        query_rows = random.randint(0, 5000) + 20000
        stream_count = 0
        row_count = 0
        query = random_query(query_rows)
        stream = test_client.query_np_stream(query, settings={'max_block_size': 5000})
        with stream:
            for np_array in stream:
                stream_count += 1
                row_count += np_array.shape[0]
        assert row_count == query_rows
        assert stream_count > 2
