#!/usr/bin/env python3 -u

import pandas as pd
import timeplus_connect


create_table_sql = """
CREATE STREAM pandas_example
(
    `timeseries` DateTime('UTC'),
    `int_value` int32,
    `str_value` string,
    `float_value` float64
)
"""


def write_pandas_df():
    client = timeplus_connect.get_client(host='localhost', port='3218', user='default', password='')
    client.command('DROP STREAM IF EXISTS pandas_example')
    client.command(create_table_sql)
    df = pd.DataFrame({'timeseries': ['04/03/2022 10:00:11', '05/03/2022 11:15:44', '06/03/2022 17:14:00'],
                      'int_value': [16, 19, 11],
                       'str_value': ['String One', 'String Two', 'A Third String'],
                       'float_value': [2344.288, -73002.4444, 3.14159]})
    df['timeseries'] = pd.to_datetime(df['timeseries'])
    client.insert_df('pandas_example', df)
    result_df = client.query_df('SELECT * FROM pandas_example WHERE _tp_time > earliest_ts() LIMIT 1')
    print()
    print(result_df.dtypes)
    print()
    print(result_df)


if __name__ == '__main__':
    write_pandas_df()
