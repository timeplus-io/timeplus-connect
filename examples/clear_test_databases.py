#!/usr/bin/env python3 -u

import os

import timeplus_connect


def main():
    host = os.getenv('CLICKHOUSE_CONNECT_TEST_HOST', 'localhost')
    port = int(os.getenv('CLICKHOUSE_CONNECT_TEST_PORT', '3218'))
    password = os.getenv('CLICKHOUSE_CONNECT_TEST_PASSWORD', '')
    client = timeplus_connect.get_client(host=host, port=port, password=password)
    database_result = client.query("SELECT name FROM system.databases WHERE name ilike '%test%'").result_rows
    for database_row in database_result:
        database:str = database_row[0]
        if database.startswith('dbt_clickhouse') or database.startswith('timeplus_connect'):
            print(f'DROPPING DATABASE `{database}`')
            client.command(f'DROP DATABASE IF EXISTS {database}')


if __name__ == '__main__':
    main()
