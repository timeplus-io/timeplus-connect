from pathlib import Path
from typing import Callable
from time import sleep

from timeplus_connect.driver import Client


def test_raw_insert(test_client: Client, table_context: Callable):
    with table_context('test_raw_insert', ["`weir'd` string", 'value string']):
        csv = 'value1\nvalue2'
        test_client.raw_insert('test_raw_insert', ['"weir\'d"'], csv.encode(), fmt='CSV')
        sleep(3)
        result = test_client.query('SELECT * except _tp_time FROM table(test_raw_insert)')
        assert result.result_set[1][0] == 'value2'

        test_client.command('TRUNCATE test_raw_insert')
        tsv = 'weird1\tvalue__`2\nweird2\tvalue77'
        test_client.raw_insert('test_raw_insert', ["`weir'd`", 'value'], tsv, fmt='TSV')
        sleep(3)
        result = test_client.query('SELECT * except _tp_time FROM table(test_raw_insert)')
        assert result.result_set[0][1] == 'value__`2'
        assert result.result_set[1][1] == 'value77'


def test_raw_insert_compression(test_client: Client, table_context: Callable):
    data_file = f'{Path(__file__).parent}/movies.csv.gz'
    with open(data_file, mode='rb') as movies_file:
        data = movies_file.read()
    with table_context('test_gzip_movies', ['movie string', 'year uint16', 'rating Decimal32(3)']):
        test_client.raw_insert('test_gzip_movies', ['movie', 'year', 'rating'], data, fmt='CSV', compression='gzip',
                                               settings={'input_format_allow_errors_ratio': .2,
                                                         'input_format_allow_errors_num': 5}
                                               )
        sleep(3)
        res = test_client.query(
            'SELECT count() as count, sum(rating) as rating, max(year) as year FROM table(test_gzip_movies)').first_item
        assert res['count'] == 248
        assert res['year'] == 2022
