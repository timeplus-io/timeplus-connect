import os
import random
from datetime import datetime, date
from typing import Callable
from io import StringIO
from time import sleep

import pytest

from timeplus_connect.driver import Client
from timeplus_connect.driver.exceptions import DataError
from timeplus_connect.driver.options import np, pd
from tests.helpers import random_query
from tests.integration_tests.conftest import TestConfig
from tests.integration_tests.datasets import null_ds, null_ds_columns, null_ds_types

pytestmark = pytest.mark.skipif(pd is None, reason='Pandas package not installed')


def test_pandas_basic(test_client: Client):
    df = test_client.query_df('SELECT * FROM system.tables')
    source_df = df.copy()
    test_client.command('DROP STREAM IF EXISTS test_system_insert_pd')
    test_client.command('CREATE STREAM test_system_insert_pd as system.tables')
    test_client.insert_df('test_system_insert_pd', df)
    sleep(3)
    new_df = test_client.query_df('SELECT * except _tp_time FROM table(test_system_insert_pd)')
    test_client.command('DROP STREAM IF EXISTS test_system_insert_pd')
    assert new_df.columns.all() == df.columns.all()
    assert df.equals(source_df)
    df = test_client.query_df("SELECT * FROM system.tables")
    assert len(df) >= 0
    assert isinstance(df, pd.DataFrame)


def test_pandas_nulls(test_client: Client, table_context: Callable):
    df = pd.DataFrame(null_ds, columns=['key', 'num', 'flt', 'str', 'dt', 'd'])
    source_df = df.copy()
    insert_columns = ['key', 'num', 'flt', 'str', 'dt', 'd']
    with table_context('test_pandas_nulls_bad', ['key string', 'num int32', 'flt float32',
                                                 'str string', 'dt DateTime', 'd Date']):

        try:
            test_client.insert_df('test_pandas_nulls_bad', df, column_names=insert_columns)
        except DataError:
            pass
    with table_context('test_pandas_nulls_good',
                       ['key string', 'num nullable(int32)', 'flt nullable(float32)',
                        'str nullable(string)', "dt nullable(DateTime('America/Denver'))", 'd nullable(Date)']):
        test_client.insert_df('test_pandas_nulls_good', df, column_names=insert_columns)
        result_df = test_client.query_df('SELECT * except _tp_time FROM test_pandas_nulls_good WHERE _tp_time > earliest_ts() LIMIT 4')
        assert result_df.iloc[0]['num'] == 1000
        assert pd.isna(result_df.iloc[2]['num'])
        assert result_df.iloc[1]['d'] == pd.Timestamp(year=1976, month=5, day=5)
        assert pd.isna(result_df.iloc[0]['d'])
        assert pd.isna(result_df.iloc[1]['dt'])
        assert pd.isna(result_df.iloc[2]['flt'])
        assert pd.isna(result_df.iloc[2]['num'])
        assert pd.isnull(result_df.iloc[3]['flt'])
        assert result_df['num'].dtype.name == 'Int32'
        if test_client.protocol_version:
            assert isinstance(result_df['dt'].dtype, pd.core.dtypes.dtypes.DatetimeTZDtype)
        assert result_df.iloc[2]['str'] == 'value3'
        assert df.equals(source_df)


def test_pandas_all_null_float(test_client: Client):
    df = test_client.query_df("SELECT number, cast(NULL, 'nullable(float64)') as flt FROM numbers(500)")
    assert df['flt'].dtype.name == 'float64'


def test_pandas_csv(test_client: Client, table_context: Callable):
    csv = """
key,num,flt,str,dt,d
key1,555,25.44,string1,2022-11-22 15:00:44,2001-02-14
key2,6666,,string2,,
"""
    csv_file = StringIO(csv)
    df = pd.read_csv(csv_file, parse_dates=['dt', 'd'])
    df = df[['num', 'flt']].astype('float32')
    source_df = df.copy()
    with table_context('test_pandas_csv', null_ds_columns, null_ds_types):
        test_client.insert_df('test_pandas_csv', df)
        result_df = test_client.query_df('SELECT * except _tp_time FROM test_pandas_csv WHERE _tp_time > earliest_ts() LIMIT 2')
        assert np.isclose(result_df.iloc[0]['flt'], 25.44)
        assert pd.isna(result_df.iloc[1]['flt'])
        result_df = test_client.query('SELECT * except _tp_time FROM test_pandas_csv WHERE _tp_time > earliest_ts() LIMIT 2')
        assert pd.isna(result_df.result_set[1][2])
        assert df.equals(source_df)


def test_pandas_context_inserts(test_client: Client, table_context: Callable):
    with table_context('test_pandas_multiple', null_ds_columns, null_ds_types):
        df = pd.DataFrame(null_ds, columns=null_ds_columns)
        source_df = df.copy()
        insert_context = test_client.create_insert_context('test_pandas_multiple', df.columns)
        insert_context.data = df
        test_client.data_insert(insert_context)
        sleep(3)
        assert test_client.command('SELECT count() FROM table(test_pandas_multiple)') == 4
        next_df = pd.DataFrame(
            [['key4', -415, None, 'value4', datetime(2022, 7, 4, 15, 33, 4, 5233), date(1999, 12, 31)]],
            columns=null_ds_columns)
        test_client.insert_df(df=next_df, context=insert_context)
        sleep(3)
        assert test_client.command('SELECT count() FROM table(test_pandas_multiple)') == 5
        assert df.equals(source_df)


def test_pandas_low_card(test_client: Client, table_context: Callable):
    with table_context('test_pandas_low_card', ['key string',
                                                'value low_cardinality(nullable(string))',
                                                'date_value low_cardinality(nullable(DateTime))',
                                                'int_value low_cardinality(nullable(int32))']):
        df = pd.DataFrame([
            ['key1', 'test_string_0', datetime(2022, 10, 15, 4, 25), -372],
            ['key2', 'test_string_1', datetime.now(), 4777288],
            ['key3', None, datetime.now(), 4777288],
            ['key4', 'test_string', pd.NaT, -5837274],
            ['key5', pd.NA, pd.NA, None]
        ],
            columns=['key', 'value', 'date_value', 'int_value'])
        source_df = df.copy()
        test_client.insert_df('test_pandas_low_card', df)
        result_df = test_client.query_df(
            'SELECT * except _tp_time FROM test_pandas_low_card WHERE _tp_time > earliest_ts() LIMIT 5',
            use_none=True
        )
        assert result_df.iloc[0]['value'] == 'test_string_0'
        assert result_df.iloc[1]['value'] == 'test_string_1'
        assert result_df.iloc[0]['date_value'] == pd.Timestamp(2022, 10, 15, 4, 25)
        assert result_df.iloc[1]['int_value'] == 4777288
        assert pd.isna(result_df.iloc[3]['date_value'])
        assert pd.isna(result_df.iloc[2]['value'])
        assert pd.api.types.is_datetime64_any_dtype(result_df['date_value'].dtype)
        assert df.equals(source_df)


def test_pandas_large_types(test_client: Client, table_context: Callable):
    columns = ['key string', 'value int256', 'u_value uint256'
               ]
    key2_value = 30000000000000000000000000000000000
    # if not test_client.min_version('21'):
    #     columns = ['key string', 'value int64']
    #     key2_value = 3000000000000000000
    with table_context('test_pandas_big_int', columns):
        df = pd.DataFrame([['key1', 2000, 50], ['key2', key2_value, 70], ['key3', -2350, 70]], columns=['key', 'value', 'u_value'])
        source_df = df.copy()
        test_client.insert_df('test_pandas_big_int', df)
        result_df = test_client.query_df('SELECT * except _tp_time FROM test_pandas_big_int WHERE _tp_time > earliest_ts() LIMIT 3')
        assert result_df.iloc[0]['value'] == 2000
        assert result_df.iloc[1]['value'] == key2_value
        assert df.equals(source_df)


def test_pandas_enums(test_client: Client, table_context: Callable):
    columns = ['key string', "value enum8('Moscow' = 0, 'Rostov' = 1, 'Kiev' = 2)",
               "null_value nullable(enum8('red'=0,'blue'=5,'yellow'=10))"]
    with table_context('test_pandas_enums', columns):
        df = pd.DataFrame([['key1', 1, 0], ['key2', 0, None]], columns=['key', 'value', 'null_value'])
        source_df = df.copy()
        test_client.insert_df('test_pandas_enums', df)
        result_df = test_client.query_df(
            'SELECT * except _tp_time FROM test_pandas_enums WHERE _tp_time > earliest_ts() ORDER BY key LIMIT 2'
        )
        assert result_df.iloc[0]['value'] == 'Rostov'
        assert result_df.iloc[1]['value'] == 'Moscow'
        assert result_df.iloc[1]['null_value'] is None
        assert result_df.iloc[0]['null_value'] == 'red'
        assert df.equals(source_df)
        df = pd.DataFrame([['key3', 'Rostov', 'blue'], ['key4', 'Moscow', None]], columns=['key', 'value', 'null_value'])
        test_client.insert_df('test_pandas_enums', df)
        result_df = test_client.query_df(
            'SELECT * except _tp_time FROM test_pandas_enums WHERE _tp_time > earliest_ts() ORDER BY key LIMIT 4'
        )
        assert result_df.iloc[2]['key'] == 'key3'
        assert result_df.iloc[2]['value'] == 'Rostov'
        assert result_df.iloc[3]['value'] == 'Moscow'
        assert result_df.iloc[2]['null_value'] == 'blue'
        assert result_df.iloc[3]['null_value'] is None


def test_pandas_datetime64(test_client: Client, table_context: Callable):
    # if not test_client.min_version('20'):
    #     pytest.skip(f'DateTime64 not supported in this server version {test_client.server_version}')
    nano_timestamp = pd.Timestamp(1992, 11, 6, 12, 50, 40, 7420, 44)
    milli_timestamp = pd.Timestamp(2022, 5, 3, 10, 44, 10, 55000)
    chicago_timestamp = milli_timestamp.tz_localize('America/Chicago')
    with table_context('test_pandas_dt64', ['key string',
                                            'nanos DateTime64(9)',
                                            'millis DateTime64(3)',
                                            "chicago DateTime64(3, 'America/Chicago')"]):
        now = datetime.now()
        df = pd.DataFrame([['key1', now, now, now],
                           ['key2', nano_timestamp, milli_timestamp, chicago_timestamp]],
                          columns=['key', 'nanos', 'millis', 'chicago'])
        source_df = df.copy()
        test_client.insert_df('test_pandas_dt64', df)
        result_df = test_client.query_df('SELECT * except _tp_time FROM test_pandas_dt64 WHERE _tp_time > earliest_ts() LIMIT 2')
        assert result_df.iloc[0]['nanos'] == now
        assert result_df.iloc[1]['nanos'] == nano_timestamp
        assert result_df.iloc[1]['millis'] == milli_timestamp
        assert result_df.iloc[1]['chicago'] == chicago_timestamp
        assert isinstance(result_df['chicago'].dtype, pd.core.dtypes.dtypes.DatetimeTZDtype)
        test_dt = np.array(['2017-11-22 15:42:58.270000+00:00'][0])
        assert df.equals(source_df)
        df = pd.DataFrame([['key3', pd.to_datetime(test_dt)]], columns=['key', 'nanos'])
        test_client.insert_df('test_pandas_dt64', df)
        result_df = test_client.query_df(
            'SELECT * except _tp_time FROM test_pandas_dt64 WHERE key = %s AND _tp_time > earliest_ts() LIMIT 1',
            parameters=('key3',)
        )
        assert result_df.iloc[0]['nanos'].second == 58


def test_pandas_streams(test_client: Client):
    # if not test_client.min_version('22'):
    #     pytest.skip(f'generateRandom is not supported in this server version {test_client.server_version}')
    runs = os.environ.get('CLICKHOUSE_CONNECT_TEST_FUZZ', '250')
    for _ in range(int(runs) // 2):
        query_rows = random.randint(0, 5000) + 20000
        stream_count = 0
        row_count = 0
        query = random_query(query_rows, date32=False)
        stream = test_client.query_df_stream(query, settings={'max_block_size': 5000})
        with stream:
            for df in stream:
                stream_count += 1
                row_count += len(df)
        assert row_count == query_rows
        assert stream_count > 2


def test_pandas_date(test_client: Client, table_context:Callable):
    with table_context('test_pandas_date', ['key uint32', 'dt Date', 'null_dt nullable(Date)']):
        df = pd.DataFrame([[1, pd.Timestamp(1992, 10, 15), pd.Timestamp(2023, 5, 4)],
                           [2, pd.Timestamp(2088, 1, 31), pd.NaT],
                           [3, pd.Timestamp(1971, 4, 15), pd.Timestamp(2101, 12, 31)]],
                          columns=['key', 'dt', 'null_dt'])
        test_client.insert_df('test_pandas_date', df)
        result_df = test_client.query_df('SELECT * except _tp_time FROM test_pandas_date WHERE _tp_time > earliest_ts() LIMIT 3')
        assert result_df.iloc[0]['dt'] == pd.Timestamp(1992, 10, 15)
        assert result_df.iloc[1]['dt'] == pd.Timestamp(2088, 1, 31)
        assert result_df.iloc[0]['null_dt'] == pd.Timestamp(2023, 5, 4)
        assert pd.isnull(result_df.iloc[1]['null_dt'])
        assert result_df.iloc[2]['null_dt'] == pd.Timestamp(2101, 12, 31)


def test_pandas_date32(test_client: Client, table_context:Callable):
    with table_context('test_pandas_date32', ['key uint32', 'dt Date32', 'null_dt nullable(Date32)']):
        df = pd.DataFrame([[1, pd.Timestamp(1992, 10, 15), pd.Timestamp(2023, 5, 4)],
                           [2, pd.Timestamp(2088, 1, 31), pd.NaT],
                           [3, pd.Timestamp(1968, 4, 15), pd.Timestamp(2101, 12, 31)]],
                          columns=['key', 'dt', 'null_dt'])
        test_client.insert_df('test_pandas_date32', df)
        result_df = test_client.query_df('SELECT * except _tp_time FROM test_pandas_date32 WHERE _tp_time > earliest_ts() LIMIT 3')
        assert result_df.iloc[1]['dt'] == pd.Timestamp(2088, 1, 31)
        assert result_df.iloc[0]['dt'] == pd.Timestamp(1992, 10, 15)
        assert result_df.iloc[0]['null_dt'] == pd.Timestamp(2023, 5, 4)
        assert pd.isnull(result_df.iloc[1]['null_dt'])
        assert result_df.iloc[2]['null_dt'] == pd.Timestamp(2101, 12, 31)
        assert result_df.iloc[2]['dt'] == pd.Timestamp(1968, 4, 15)


def test_pandas_row_df(test_client: Client, table_context:Callable):
    with table_context('test_pandas_row_df', ['key uint64', 'dt DateTime64(6)', 'fs fixed_string(5)']):
        df = pd.DataFrame({'key': [1, 2],
                          'dt': [pd.Timestamp(2023, 5, 4, 10, 20), pd.Timestamp(2023, 10, 15, 14, 50, 2, 4038)],
                           'fs': ['seven', 'bit']})
        df = df.iloc[1:]
        source_df = df.copy()
        test_client.insert_df('test_pandas_row_df', df)
        result_df = test_client.query_df(
            'SELECT * except _tp_time FROM test_pandas_row_df WHERE _tp_time > earliest_ts() LIMIT 1',
            column_formats={'fs': 'string'}
        )
        assert str(result_df.dtypes.iloc[2]) == 'string'
        assert result_df.iloc[0]['key'] == 2
        assert result_df.iloc[0]['dt'] == pd.Timestamp(2023, 10, 15, 14, 50, 2, 4038)
        assert result_df.iloc[0]['fs'] == 'bit'
        assert len(result_df) == 1
        assert source_df.equals(df)


def test_pandas_null_strings(test_client: Client, table_context:Callable):
    with table_context('test_pandas_null_strings', ['id string', 'test_col low_cardinality(string)']):
        row = {'id': 'id', 'test_col': None}
        df = pd.DataFrame([row])
        assert df['test_col'].isnull().values.all()
        with pytest.raises(DataError):
            test_client.insert_df('test_pandas_null_strings', df)
        row2 = {'id': 'id2', 'test_col': 'val'}
        df = pd.DataFrame([row, row2])
        with pytest.raises(DataError):
            test_client.insert_df('test_pandas_null_strings', df)


def test_pandas_small_blocks(test_config: TestConfig, test_client: Client):
    if test_config.cloud:
        pytest.skip('Skipping performance test in ClickHouse Cloud')
    res = test_client.query_df('SELECT number, random_string(512) FROM numbers(1000000)',
                               settings={'max_block_size': 250})
    assert len(res) == 1000000
