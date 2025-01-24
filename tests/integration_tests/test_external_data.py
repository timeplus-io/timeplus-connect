from pathlib import Path

import pytest

from timeplus_connect.driver import Client
from timeplus_connect.driver.external import ExternalData
from timeplus_connect.driver.options import arrow
from tests.integration_tests.conftest import TestConfig

ext_settings = {'input_format_allow_errors_num': 10, 'input_format_allow_errors_ratio': .2}


def test_external_simple(test_client: Client):
    data_file = f'{Path(__file__).parent}/movies.csv'
    data = ExternalData(data_file, fmt='CSVWithNames',
                        structure=['movie string', 'year uint16', 'rating Decimal32(3)'])
    result = test_client.query('SELECT * except _tp_time FROM movies ORDER BY movie',
                               external_data=data,
                               settings=ext_settings).result_rows
    assert result[0][0] == '12 Angry Men'


def test_external_arrow(test_client: Client):
    if not arrow:
        pytest.skip('PyArrow package not available')
    if not test_client.min_version('21'):
        pytest.skip(f'PyArrow is not supported in this server version {test_client.server_version}')
    data_file = f'{Path(__file__).parent}/movies.csv'
    data = ExternalData(data_file, fmt='CSVWithNames',
                        structure=['movie string', 'year uint16', 'rating Decimal32(3)'])
    result = test_client.query_arrow('SELECT * except _tp_time FROM movies ORDER BY movie',
                                     external_data=data,
                                     settings=ext_settings)
    assert str(result[0][0]) == '12 Angry Men'


def test_external_multiple(test_client: Client):
    movies_file = f'{Path(__file__).parent}/movies.csv'
    data = ExternalData(movies_file, fmt='CSVWithNames',
                        structure=['movie string', 'year uint16', 'rating Decimal32(3)'])
    actors_file = f'{Path(__file__).parent}/actors.csv'
    data.add_file(actors_file, fmt='CSV', types='string,uint16,string')
    result = test_client.query('SELECT * except _tp_time FROM actors;', external_data=data, settings={
        'input_format_allow_errors_num': 10, 'input_format_allow_errors_ratio': .2}).result_rows
    assert result[1][1] == 1940
    result = test_client.query(
        'SELECT _1, movie FROM actors INNER JOIN movies ON actors._3 = movies.movie AND actors._2 = 1940',
        external_data=data,
        settings=ext_settings).result_rows
    assert len(result) == 1
    assert result[0][1] == 'Scarface'


def test_external_parquet(test_config: TestConfig, test_client: Client):
    if test_config.cloud:
        pytest.skip('External data join not working in SMT, skipping')
    movies_file = f'{Path(__file__).parent}/movies.parquet'
    test_client.command('DROP STREAM IF EXISTS movies')
    test_client.command('DROP STREAM IF EXISTS num')
    test_client.command("""
CREATE STREAM IF NOT EXISTS num (number uint64, t string)
ENGINE = MergeTree
ORDER BY number""")
    test_client.command("""
INSERT INTO num SELECT number, concat(to_string(number), 'x') as t
FROM numbers(2500)
WHERE (number > 1950) AND (number < 2025)
    """)
    data = ExternalData(movies_file, fmt='Parquet', structure=['movie string', 'year uint16', 'rating float64'])
    result = test_client.query(
        "SELECT * except _tp_time FROM movies INNER JOIN num ON movies.year = number AND t = '2000x' ORDER BY movie",
        # Unknown setting output_format_parquet_string_as_string
        # settings={'output_format_parquet_string_as_string': 1},
        external_data=data).result_rows
    assert len(result) == 5
    assert result[2][0] == 'Memento'
    test_client.command('DROP STREAM num')


def test_external_binary(test_client: Client):
    actors = 'Robert Redford\t1936\tThe Sting\nAl Pacino\t1940\tScarface'.encode()
    data = ExternalData(file_name='actors.csv', data=actors,
                        structure='name string, birth_year uint16, movie string')
    result = test_client.query('SELECT * except _tp_time FROM actors ORDER BY birth_year DESC', external_data=data).result_rows
    assert len(result) == 2
    assert result[1][2] == 'The Sting'


def test_external_empty_binary(test_client: Client):
    data = ExternalData(file_name='empty.csv', data=b'', structure='name string')
    result = test_client.query('SELECT * except _tp_time FROM empty', external_data=data).result_rows
    assert len(result) == 0


def test_external_raw(test_client: Client):
    movies_file = f'{Path(__file__).parent}/movies.parquet'
    data = ExternalData(movies_file, fmt='Parquet', structure=['movie string', 'year uint16', 'rating float64'])
    result = test_client.raw_query('SELECT avg(rating) FROM movies', external_data=data)
    assert '8.25' == result.decode()[0:4]


def test_external_command(test_client: Client):
    movies_file = f'{Path(__file__).parent}/movies.parquet'
    data = ExternalData(movies_file, fmt='Parquet', structure=['movie string', 'year uint16', 'rating float64'])
    result = test_client.command('SELECT avg(rating) FROM movies', external_data=data)
    assert '8.25' == result[0:4]

    test_client.command('DROP STREAM IF EXISTS movies_ext')
    if test_client.min_version('22.8'):
        query_result = test_client.query('CREATE STREAM movies_ext ENGINE MergeTree() ORDER BY tuple() EMPTY ' +
                                         'AS SELECT * except _tp_time FROM movies', external_data=data)
        assert 'query_id' in query_result.first_item
        test_client.raw_query('INSERT INTO movies_ext SELECT * except _tp_time FROM movies', external_data=data)
        assert 250 == test_client.command('SELECT COUNT() FROM movies_ext')
