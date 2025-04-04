
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql import text

from timeplus_connect import dbapi

from timeplus_connect.cc_sqlalchemy.inspector import TpInspector
from timeplus_connect.cc_sqlalchemy.sql import full_table
from timeplus_connect.cc_sqlalchemy.sql.ddlcompiler import TpDDLCompiler
from timeplus_connect.cc_sqlalchemy import ischema_names, dialect_name
from timeplus_connect.cc_sqlalchemy.sql.preparer import TpIdentifierPreparer
from timeplus_connect.driver.binding import quote_identifier, format_str


# pylint: disable=too-many-public-methods,no-self-use,unused-argument
class TimeplusDialect(DefaultDialect):
    """
    See :py:class:`sqlalchemy.engine.interfaces`
    """
    name = dialect_name
    driver = 'connect'

    default_schema_name = 'default'
    supports_native_decimal = True
    supports_native_boolean = True
    supports_statement_cache = False
    returns_unicode_strings = True
    postfetch_lastrowid = False
    ddl_compiler = TpDDLCompiler
    preparer = TpIdentifierPreparer
    description_encoding = None
    max_identifier_length = 127
    ischema_names = ischema_names
    inspector = TpInspector

    @classmethod
    def import_dbapi(cls):
        return dbapi

    def initialize(self, connection):
        pass

    @staticmethod
    def get_schema_names(connection, **_):
        query = text('SHOW DATABASES')
        return [row.name for row in connection.execute(query)]

    @staticmethod
    def has_database(connection, db_name):
        return (connection.execute('SELECT name FROM system.databases ' +
                                   f'WHERE name = {format_str(db_name)}')).rowcount > 0

    def get_table_names(self, connection, schema=None, **kw):
        cmd = text('SHOW STREAMS')  # Wrap in text() to make it an executable SQLAlchemy statement
        if schema:
            cmd = text(f"SHOW STREAMS FROM {quote_identifier(schema)}")  # Ensure schema is properly quoted

        return [row.name for row in connection.execute(cmd)]

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        inspector = self.inspector(connection)
        return inspector.get_columns(table_name, schema, **kwargs)

    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        return []

    #  pylint: disable=arguments-renamed
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        return []

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_temp_table_names(self, connection, schema=None, **kw):
        return []

    def get_view_names(self, connection, schema=None, **kw):
        return []

    def get_temp_view_names(self, connection, schema=None, **kw):
        return []

    def get_view_definition(self, connection, view_name, schema=None, **kw):
        pass

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        return []

    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        return []

    def has_table(self, connection, table_name, schema=None, **_kw):
        result = connection.execute(f'EXISTS STREAM {full_table(table_name, schema)}')
        row = result.fetchone()
        return row[0] == 1

    def has_sequence(self, connection, sequence_name, schema=None, **_kw):
        return False

    def do_begin_twophase(self, connection, xid):
        raise NotImplementedError

    def do_prepare_twophase(self, connection, xid):
        raise NotImplementedError

    def do_rollback_twophase(self, connection, xid, is_prepared=True, recover=False):
        raise NotImplementedError

    def do_commit_twophase(self, connection, xid, is_prepared=True, recover=False):
        raise NotImplementedError

    def do_recover_twophase(self, connection):
        raise NotImplementedError

    def set_isolation_level(self, dbapi_conn, level):
        pass

    def get_isolation_level(self, dbapi_conn):
        return None
