import timeplus_connect

client: timeplus_connect.driver.Client


def inserted_nested_flat():
    client.command('DROP STREAM IF EXISTS test_nested_flat')
    client.command('SET flatten_nested = 1')
    client.command(
"""
CREATE STREAM test_nested_flat
(
    `key` uint32,
    `value` nested(str string, int32 int32)
)
""")
    result = client.query('DESCRIBE STREAM test_nested_flat')
    print(result.column_names[0:2])
    print(result.result_columns[0:2])

    # Note the nested 'value' column is inserted as two parallel arrays of values
    # into their own columns of the form `col_name.key_name` with array data types
    data = [[1, ['string_1', 'string_2'], [20, 30]],
            [2, ['string_3', 'string_4'], [40, 50]]
            ]
    client.insert('test_nested_flat', data,
                  column_names=['key', 'value.str', 'value.int32'],
                  column_type_names=['uint32', 'array(string)', 'array(int32)'])

    result = client.query('SELECT * FROM test_nested_flat WHERE _tp_time > earliest_ts() LIMIT 2')
    print(result.column_names)
    print(result.result_columns)
    client.command('DROP STREAM test_nested_flat')


def insert_nested_not_flat():
    client.command('DROP STREAM IF EXISTS test_nested_not_flat')
    client.command('SET flatten_nested = 0')
    client.command(
"""
CREATE STREAM test_nested_not_flat
(
    `key` uint32,
    `value` nested(str string, int32 int32)
)
""")
    result = client.query('DESCRIBE test_nested_not_flat')
    print (result.column_names[0:2])
    print (result.result_columns[0:2])

    # Note the nested 'value' column is inserted as a list of dictionaries for each row
    data = [[1, [{'str': 'nested_string_1', 'int32': 20},
                {'str': 'nested_string_2', 'int32': 30}]],
            [2, [{'str': 'nested_string_3', 'int32': 40},
                {'str': 'nested_string_4', 'int32': 50}]]
            ]
    client.insert('test_nested_not_flat', data,
                  column_names=['key', 'value'],
                  column_type_names=['uint32', 'nested(str string, int32 int32)'])

    result = client.query('SELECT * FROM test_nested_not_flat WHERE _tp_time > earliest_ts() LIMIT 2')
    print(result.column_names)
    print(result.result_columns)
    client.command('DROP STREAM test_nested_not_flat')


def main():
    global client  # pylint:  disable=global-statement
    client = timeplus_connect.get_client()
    print ('nested example flatten_nested = 1 (Default)')
    inserted_nested_flat()
    print('\n\nnested example flatten_nested = 0')
    insert_nested_not_flat()


if __name__ == '__main__':
    main()
