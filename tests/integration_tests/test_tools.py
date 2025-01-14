from pathlib import Path
from typing import Callable
from time import sleep

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.tools import insert_file
from tests.integration_tests.conftest import TestConfig


def test_csv_upload(test_client: Client, table_context: Callable):
    data_file = f'{Path(__file__).parent}/movies.csv.gz'
    with table_context('test_csv_upload', ['movie string', 'year uint16', 'rating Decimal32(3)']):
        insert_file(test_client, 'test_csv_upload', data_file,
                                    settings={'input_format_allow_errors_ratio': .2,
                                              'input_format_allow_errors_num': 5},
                                    column_names=['movie', 'year', 'rating'])
        sleep(3)
        res = test_client.query(
            'SELECT count() as count, sum(rating) as rating, max(year) as year FROM table(test_csv_upload)').first_item
        assert res['count'] == 248
        assert res['year'] == 2022


def test_parquet_upload(test_config: TestConfig, test_client: Client, table_context: Callable):
    data_file = f'{Path(__file__).parent}/movies.parquet'
    full_table = f'{test_config.test_database}.test_parquet_upload'
    with table_context(full_table, ['movie string', 'year uint16', 'rating float64']):
        insert_file(test_client, full_table, data_file, 'Parquet', ['movie', 'year', 'rating'])
                                    # settings={'output_format_parquet_string_as_string': 1})
        sleep(3)
        res = test_client.query(
            f'SELECT count() as count, sum(rating) as rating, max(year) as year FROM table({full_table})').first_item
        assert res['count'] == 250
        assert res['year'] == 2022


def test_json_insert(test_client: Client, table_context: Callable):
    data_file = f'{Path(__file__).parent}/json_test.ndjson'
    with table_context('test_json_upload', ['key uint16', 'flt_val float64', 'int_val int8']):
        insert_file(test_client, 'test_json_upload', data_file, 'JSONEachRow', ['key', 'flt_val', 'int_val'])
        sleep(3)
        res = test_client.query('SELECT * except _tp_time FROM table(test_json_upload) ORDER BY key').result_rows
        assert res[1][0] == 17
        assert res[1][1] == 5.3
        assert res[1][2] == 121
