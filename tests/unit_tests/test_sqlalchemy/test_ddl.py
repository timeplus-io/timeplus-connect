import sqlalchemy as db
from sqlalchemy.sql.ddl import CreateTable

from timeplus_connect.cc_sqlalchemy.datatypes.sqltypes import UInt64, UInt32, DateTime
from timeplus_connect.cc_sqlalchemy.ddl.tableengine import ReplicatedMergeTree, ReplacingMergeTree
from timeplus_connect.cc_sqlalchemy.dialect import TimeplusDialect

dialect = TimeplusDialect()

replicated_mt_ddl = """\
CREATE STREAM `replicated_mt_test` (`key` uint64) Engine ReplicatedMergeTree('/clickhouse/tables/repl_mt_test',\
 '{replica}') ORDER BY key\
"""

replacing_mt_ddl = """\
CREATE STREAM `replacing_mt_test` (`key` uint32, `date` DateTime) Engine ReplacingMergeTree(date) ORDER BY key\
"""


def test_table_def():
    metadata = db.MetaData()

    table = db.Table('replicated_mt_test', metadata, db.Column('key', UInt64),
                     ReplicatedMergeTree(order_by='key', zk_path='/clickhouse/tables/repl_mt_test',
                                         replica='{replica}'))
    ddl = str(CreateTable(table).compile('', dialect=dialect))
    assert ddl == replicated_mt_ddl

    table = db.Table('replacing_mt_test', metadata, db.Column('key', UInt32), db.Column('date', DateTime),
                     ReplacingMergeTree(ver='date', order_by='key'))

    ddl = str(CreateTable(table).compile('', dialect=dialect))
    assert ddl == replacing_mt_ddl
