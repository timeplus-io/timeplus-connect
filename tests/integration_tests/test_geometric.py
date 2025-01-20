from typing import Callable
import pytest
from timeplus_connect.driver import Client

@pytest.mark.skip("proton doesn't support geometric type")
def test_point_column(test_client: Client, table_context: Callable):
    with table_context('point_column_test', ['key int32', 'point Point']):
        data = [[1, (3.55, 3.55)], [2, (4.55, 4.55)]]
        test_client.insert('point_column_test', data)

        query_result = test_client.query('SELECT * except _tp_time FROM point_column_test ORDER BY key').result_rows
        assert len(query_result) == 2
        assert query_result[0] == (1, (3.55, 3.55))
        assert query_result[1] == (2, (4.55, 4.55))


@pytest.mark.skip("proton doesn't support geometric type")
def test_ring_column(test_client: Client, table_context: Callable):
    with table_context('ring_column_test', ['key int32', 'ring Ring']):
        data = [[1, [(5.522, 58.472),(3.55, 3.55)]], [2, [(4.55, 4.55)]]]
        test_client.insert('ring_column_test', data)

        query_result = test_client.query('SELECT * except _tp_time FROM ring_column_test ORDER BY key').result_rows
        assert len(query_result) == 2
        assert query_result[0] == (1, [(5.522, 58.472),(3.55, 3.55)])
        assert query_result[1] == (2, [(4.55, 4.55)])


@pytest.mark.skip("proton doesn't support geometric type")
def test_polygon_column(test_client: Client, table_context: Callable):
    with table_context('polygon_column_test', ['key int32', 'polygon Polygon']):
        res = test_client.query("SELECT readWKTPolygon('POLYGON ((-64.8 32.3, -65.5 18.3, -80.3 25.2, -64.8 32.3))') as polygon")
        pg = res.first_row[0]
        test_client.insert('polygon_column_test', [(1, pg), (4, pg)])
        query_result = test_client.query('SELECT key, polygon FROM polygon_column_test WHERE key = 4')
        assert query_result.first_row[1] == pg
