from llama_index.readers.database import DatabaseReader
import timeplus_connect


# Ensure the timeplus-connect driver is registered
TIMEPLUS_URI = "timeplus://user:password@localhost:8123"
db_reader = DatabaseReader(
    uri=TIMEPLUS_URI  # Use the explicit SQLAlchemy engine
)

print(f"db reader type: {type(db_reader)}")
print(type(db_reader.load_data))

### SQLDatabase class ###
# db.sql is an instance of SQLDatabase:
print(type(db_reader.sql_database))
# SQLDatabase available methods:
print(type(db_reader.sql_database.from_uri))
print(type(db_reader.sql_database.get_single_table_info))
print(type(db_reader.sql_database.get_table_columns))
print(type(db_reader.sql_database.get_usable_table_names))
print(type(db_reader.sql_database.insert_into_table))
print(type(db_reader.sql_database.run_sql))
# SQLDatabase available properties:
print(type(db_reader.sql_database.dialect))
print(type(db_reader.sql_database.engine))