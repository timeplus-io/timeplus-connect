"""
AsyncClient tests that verify that the wrapper for each method is working correctly.
"""

from typing import Callable

import numpy as np
import pandas as pd
import pytest

from timeplus_connect.driver.options import arrow

from timeplus_connect.driver import AsyncClient


@pytest.mark.asyncio
async def test_client_settings(test_async_client: AsyncClient):
    key = 'prefer_column_name_to_alias'
    value = '1'
    test_async_client.set_client_setting(key, value)
    assert test_async_client.get_client_setting(key) == value


@pytest.mark.asyncio
async def test_min_version(test_async_client: AsyncClient):
    assert test_async_client.min_version('1') is True
    # assert test_async_client.min_version('22.4') is True
    assert test_async_client.min_version('99999') is False


@pytest.mark.asyncio
async def test_query(test_async_client: AsyncClient):
    result = await test_async_client.query('SELECT * except _tp_time FROM system.tables')
    assert len(result.result_set) > 0
    assert result.row_count > 0
    assert result.first_item == next(result.named_results())


stream_query = 'SELECT number, random_string_utf8(50) FROM numbers(10000)'
stream_settings = {'max_block_size': 4000}


# pylint: disable=duplicate-code
@pytest.mark.asyncio
async def test_query_column_block_stream(test_async_client: AsyncClient):
    block_stream = await test_async_client.query_column_block_stream(stream_query, settings=stream_settings)
    total = 0
    block_count = 0
    with block_stream:
        for block in block_stream:
            block_count += 1
            total += sum(block[0])
    assert total == 49995000
    assert block_count > 1


# pylint: disable=duplicate-code
@pytest.mark.asyncio
async def test_query_row_block_stream(test_async_client: AsyncClient):
    block_stream = await test_async_client.query_row_block_stream(stream_query, settings=stream_settings)
    total = 0
    block_count = 0
    with block_stream:
        for block in block_stream:
            block_count += 1
            for row in block:
                total += row[0]
    assert total == 49995000
    assert block_count > 1


@pytest.mark.asyncio
async def test_query_rows_stream(test_async_client: AsyncClient):
    row_stream = await test_async_client.query_rows_stream('SELECT number FROM numbers(10000)')
    total = 0
    with row_stream:
        for row in row_stream:
            total += row[0]
    assert total == 49995000


@pytest.mark.asyncio
async def test_raw_query(test_async_client: AsyncClient):
    result = await test_async_client.raw_query('SELECT 42')
    assert result == b'42\n'


@pytest.mark.asyncio
async def test_raw_stream(test_async_client: AsyncClient):
    stream = await test_async_client.raw_stream('SELECT 42')
    result = b''
    with stream:
        for chunk in stream:
            result += chunk
    assert result == b'42\n'


@pytest.mark.asyncio
async def test_query_np(test_async_client: AsyncClient):
    result = await test_async_client.query_np('SELECT number FROM numbers(5)')
    assert isinstance(result, np.ndarray)
    assert list(result) == [[0], [1], [2], [3], [4]]


@pytest.mark.asyncio
async def test_query_np_stream(test_async_client: AsyncClient):
    stream = await test_async_client.query_np_stream('SELECT number FROM numbers(5)')
    result = np.array([])
    with stream:
        for block in stream:
            result = np.append(result, block)
    assert list(result) == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_query_df(test_async_client: AsyncClient):
    result = await test_async_client.query_df('SELECT number FROM numbers(5)')
    assert isinstance(result, pd.DataFrame)
    assert list(result['number']) == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_query_df_stream(test_async_client: AsyncClient):
    stream = await test_async_client.query_df_stream('SELECT number FROM numbers(5)')
    result = []
    with stream:
        for block in stream:
            result.append(list(block['number']))
    assert result == [[0, 1, 2, 3, 4]]


@pytest.mark.asyncio
async def test_create_query_context(test_async_client: AsyncClient):
    query_context = test_async_client.create_query_context(
        query='SELECT {k: int32}',
        parameters={'k': 42},
        column_oriented=True)
    result = await test_async_client.query(context=query_context)
    assert result.row_count == 1
    assert result.result_set == [[42]]


@pytest.mark.asyncio
async def test_query_arrow(test_async_client: AsyncClient):
    if not arrow:
        pytest.skip('PyArrow package not available')
    pytest.skip(
        "Internal type 'uint64' of a column 'number' is not supported for conversion into Arrow data format." \
        "While executing ArrowBlockOutputFormat."
    )
    result = await test_async_client.query_arrow('SELECT number::uint32 FROM numbers(5)')
    assert isinstance(result, arrow.Table)
    assert list(result[0].to_pylist()) == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_query_arrow_stream(test_async_client: AsyncClient):
    if not arrow:
        pytest.skip('PyArrow package not available')
    pytest.skip(
        "Internal type 'uint64' of a column 'number' is not supported for conversion into Arrow data format." \
        "While executing ArrowBlockOutputFormat."
    )
    stream = await test_async_client.query_arrow_stream('SELECT number FROM numbers(5)')
    result = []
    with stream:
        for block in stream:
            result.append(block[0].to_pylist())
    assert result == [[0, 1, 2, 3, 4]]


@pytest.mark.asyncio
async def test_command(test_async_client: AsyncClient):
    version = await test_async_client.command('SELECT version()')
    assert int(version.split('.')[0]) >= 1


@pytest.mark.asyncio
async def test_ping(test_async_client: AsyncClient):
    assert await test_async_client.ping() is True


@pytest.mark.asyncio
async def test_insert(test_async_client: AsyncClient, table_context: Callable):
    with table_context('test_async_client_insert', ['key uint32', 'value string']) as ctx:
        await test_async_client.insert(ctx.table, [[42, 'str_0'], [144, 'str_1']], ['key', 'value'])
        result_set = (await test_async_client.query(
            f"SELECT * except _tp_time FROM {ctx.table} WHERE _tp_time > earliest_ts() ORDER BY key ASC LIMIT 2"
        )).result_columns
        assert result_set == [[42, 144], ['str_0', 'str_1']]


@pytest.mark.asyncio
async def test_insert_df(test_async_client: AsyncClient, table_context: Callable):
    with table_context('test_async_client_insert_df', ['key uint32', 'value string']) as ctx:
        df = pd.DataFrame([[42, 'str_0'], [144, 'str_1']], columns=['key', 'value'])
        df['key'] = df['key'].astype(np.uint32)
        df['value'] = df['value'].astype('string')
        await test_async_client.insert_df(ctx.table, df)
        result_set = (await test_async_client.query(
            f"SELECT * except _tp_time FROM {ctx.table} WHERE _tp_time > earliest_ts() ORDER BY key ASC LIMIT 2"
        )).result_columns
        assert result_set == [[42, 144], ['str_0', 'str_1']]


@pytest.mark.asyncio
async def test_insert_arrow(test_async_client: AsyncClient, table_context: Callable):
    if not arrow:
        pytest.skip('PyArrow package not available')
    with table_context('test_async_client_insert_arrow', ['key uint32', 'value string']) as ctx:
        data = arrow.Table.from_arrays([arrow.array([42, 144]), arrow.array(['str_0', 'str_1'])], names=['key', 'value'])
        await test_async_client.insert_arrow(ctx.table, data)
        result_set = (await test_async_client.query(
            f"SELECT * except _tp_time FROM {ctx.table} WHERE _tp_time > earliest_ts() ORDER BY key ASC LIMIT 2"
        )).result_columns
        assert result_set == [[42, 144], ['str_0', 'str_1']]


@pytest.mark.asyncio
async def test_create_insert_context(test_async_client: AsyncClient, table_context: Callable):
    with table_context('test_async_client_create_insert_context', ['key uint32', 'value string']) as ctx:
        data = [[1, 'a'], [2, 'b']]
        insert_context = await test_async_client.create_insert_context(table=ctx.table, data=data, column_names=['key', 'value'])
        await test_async_client.insert(context=insert_context)
        result = (await test_async_client.query(
            f'SELECT * except _tp_time FROM {ctx.table} WHERE _tp_time > earliest_ts() ORDER BY key ASC LIMIT 2'
        )).result_columns
        assert result == [[1, 2], ['a', 'b']]


@pytest.mark.asyncio
async def test_data_insert(test_async_client: AsyncClient, table_context: Callable):
    with table_context('test_async_client_data_insert', ['key uint32', 'value string']) as ctx:
        df = pd.DataFrame([[42, 'str_0'], [144, 'str_1']], columns=['key', 'value'])
        insert_context = await test_async_client.create_insert_context(ctx.table, df.columns)
        insert_context.data = df
        await test_async_client.data_insert(insert_context)
        result_set = (await test_async_client.query(
            f"SELECT * except _tp_time FROM {ctx.table} WHERE _tp_time > earliest_ts() ORDER BY key ASC LIMIT 2"
        )).result_columns
        assert result_set == [[42, 144], ['str_0', 'str_1']]


@pytest.mark.asyncio
async def test_raw_insert(test_async_client: AsyncClient, table_context: Callable):
    with table_context('test_async_client_raw_insert', ['key uint32', 'value string']) as ctx:
        await test_async_client.raw_insert(table=ctx.table,
                                           column_names=['key', 'value'],
                                           insert_block='42,"foo"\n144,"bar"\n',
                                           fmt='CSV')
        result_set = (await test_async_client.query(
            f"SELECT * except _tp_time FROM {ctx.table} WHERE _tp_time > earliest_ts() ORDER BY key ASC LIMIT 2"
        )).result_columns
        assert result_set == [[42, 144], ['foo', 'bar']]
