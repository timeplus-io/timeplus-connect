import sqlalchemy.schema as sa_schema

from sqlalchemy import text
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm.exc import NoResultFound

from timeplus_connect.cc_sqlalchemy.datatypes.base import sqla_type_from_name
from timeplus_connect.cc_sqlalchemy.ddl.tableengine import build_engine
from timeplus_connect.cc_sqlalchemy.sql import full_table
from timeplus_connect.cc_sqlalchemy import dialect_name as dn

ch_col_args = ('default_type', 'codec_expression', 'ttl_expression')


def get_engine(connection, table_name, schema=None):
    if schema is None:
        schema = 'default'

    query = text(f"SELECT engine_full FROM system.tables WHERE database = '{schema}' and name = '{table_name}'")

    result_set = connection.execute(query)
    row = next(result_set, None)
    if not row:
        raise NoResultFound(f'STREAM {schema}.{table_name} does not exist')
    return build_engine(row.engine_full)


class TpInspector(Inspector):

    def reflect_table(self, table, include_columns, exclude_columns, *_args, **_kwargs):
        schema = table.schema
        for col in self.get_columns(table.name, schema):
            name = col.pop('name')
            if (include_columns and name not in include_columns) or (exclude_columns and name in exclude_columns):
                continue
            col_type = col.pop('type')
            col_args = {f'{dn}_{key}' if key in ch_col_args else key: value for key, value in col.items() if value}
            table.append_column(sa_schema.Column(name, col_type, **col_args))
        table.engine = get_engine(self.bind, table.name, schema)

    def get_columns(self, table_name, schema=None, **_kwargs):
        if schema is None:
            schema = 'default'

        table_id = full_table(table_name, schema)
        query = text(f"DESCRIBE {table_id}")

        result_set = self.bind.execute(query)
        if not result_set:
            raise NoResultFound(f'STREAM {full_table} does not exist')
        columns = []
        for row in result_set:
            sqla_type = sqla_type_from_name(row.type.replace('\n', ''))
            col = {'name': row.name,
                   'type': sqla_type,
                   'nullable': sqla_type.nullable,
                   'autoincrement': False,
                   'default': row.default_expression,
                   'default_type': row.default_type,
                   'comment': row.comment,
                   'codec_expression': row.codec_expression,
                   'ttl_expression': row.ttl_expression}
            columns.append(col)
        return columns


TpInspector.reflecttable = TpInspector.reflect_table  # Hack to provide backward compatibility for SQLAlchemy 1.3
