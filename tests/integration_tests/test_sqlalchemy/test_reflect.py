# pylint: disable=no-member
import sqlalchemy as db
from sqlalchemy.engine import Engine
import pytest

from timeplus_connect import common
from timeplus_connect.cc_sqlalchemy.datatypes.sqltypes import UInt32, SimpleAggregateFunction, Tuple


def test_basic_reflection(test_engine: Engine):
    pytest.skip("proton default user hasn't enough privileges to create table in system schema")
    common.set_setting('invalid_setting_action', 'drop')
    conn = test_engine.connect()
    metadata = db.MetaData(bind=test_engine, schema='system')
    table = db.Table('tables', metadata, autoload_with=test_engine)
    query = db.select([table.columns.create_table_query])
    result = conn.execute(query)
    rows = result.fetchmany(100)
    assert rows


def test_full_table_reflection(test_engine: Engine, test_db: str):
    common.set_setting('invalid_setting_action', 'drop')
    conn = test_engine.connect()
    conn.execute(f'DROP STREAM IF EXISTS {test_db}.reflect_test')
    conn.execute(
        f'CREATE STREAM {test_db}.reflect_test (key uint32, value fixed_string(20),'+
        'agg simple_aggregate_function(any_last, string))')
    metadata = db.MetaData(bind=test_engine, schema=test_db)
    table = db.Table('reflect_test', metadata, autoload_with=test_engine)
    assert table.columns.key.type.__class__ == UInt32
    assert table.columns.agg.type.__class__ == SimpleAggregateFunction
    # assert 'Stream' in table.engine.name


def test_types_reflection(test_engine: Engine, test_db: str):
    common.set_setting('invalid_setting_action', 'drop')
    conn = test_engine.connect()
    conn.execute(f'DROP STREAM IF EXISTS {test_db}.sqlalchemy_types_test')
    conn.execute(
        f'CREATE STREAM {test_db}.sqlalchemy_types_test (key uint32, pt tuple(int32, int32)) ' +
        'ENGINE MergeTree ORDER BY key')
    metadata = db.MetaData(bind=test_engine, schema=test_db)
    table = db.Table('sqlalchemy_types_test', metadata, autoload_with=test_engine)
    assert table.columns.key.type.__class__ == UInt32
    assert table.columns.pt.type.__class__ == Tuple
    # assert 'MergeTree' in table.engine.name


def test_table_exists(test_engine: Engine):
    pytest.skip("proton default user hasn't enough privileges to check table in system schema")
    common.set_setting('invalid_setting_action', 'drop')
    conn = test_engine.connect()
    assert test_engine.dialect.has_table(conn, 'columns', 'system')
    assert not test_engine.dialect.has_table(conn, 'nope', 'fake_db')
