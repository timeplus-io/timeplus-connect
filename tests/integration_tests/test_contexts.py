from typing import Callable
from time import sleep

from timeplus_connect.driver import Client

def test_contexts(test_client: Client, table_context: Callable):
    with table_context('test_contexts', ['key int32', 'value1 string', 'value2 string']) as ctx:
        data = [[1, 'v1', 'v2'], [2, 'v3', 'v4']]
        column_names = ['key', 'value1', 'value2']
        insert_context = test_client.create_insert_context(table=ctx.table, data=data,column_names=column_names)
        test_client.insert(context=insert_context)
        query_context = test_client.create_query_context(
            query=f'SELECT value1, value2 FROM {ctx.table} WHERE key = {{k:int32}} AND _tp_time > earliest_ts() LIMIT {{num:int32}}',
            parameters={'k': 2, 'num': 1},
            column_oriented=True)
        result = test_client.query(context=query_context)
        assert result.result_set[1][0] == 'v4'
        query_context.set_parameter('k', 1)
        result = test_client.query(context=query_context)
        assert result.row_count == 1
        assert result.result_set[1][0]

        data = [[1, 'v5', 'v6'], [2, 'v7', 'v8']]
        test_client.insert(data=data, context=insert_context)
        query_context.set_parameter('num', 2)
        result = test_client.query(context=query_context)
        assert result.row_count == 2

        insert_context.data = [[5, 'v5', 'v6'], [7, 'v7', 'v8']]
        test_client.insert(context=insert_context)
        sleep(3)
        assert test_client.command(f'SELECT count() FROM table({ctx.table})') == 6
