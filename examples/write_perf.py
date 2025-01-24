#!/usr/bin/env python -u

# pylint: disable=import-error,no-name-in-module
import time
import random
import proton_driver

import timeplus_connect
from timeplus_connect.tools.testing import TableContext


inserts = [#{'query': 'SELECT trip_id, pickup, dropoff, pickup_longitude, ' +
           #          'pickup_latitude FROM taxis ORDER BY trip_id LIMIT 5000000',
           # 'columns': 'trip_id uint32, pickup string, dropoff string,' +
           #            ' pickup_longitude float64, pickup_latitude float64'},
           {'query': 'SELECT number from numbers(5000000)',
            'columns': 'number uint64'}]

excluded = {}
tc_client = timeplus_connect.get_client(host='127.0.0.1', port=3218, username='default', password='', database='default', compress=False)
pd_client = proton_driver.Client(host='localhost')
run_id = random.randint(0, 10000000)


def write_python_columns(ix, insert):
    print('\n\ttimeplus-connect Python Insert (column oriented):')
    data = tc_client.query(insert['query']).result_columns
    table = f'perf_test_insert_{run_id}_{ix}'
    with test_ctx(table, insert) as ctx:
        start = time.time()
        tc_client.insert(table, data, ctx.column_names, column_type_names=ctx.column_types, column_oriented=True)
    _print_result(start, len(data[0]))


def write_python_rows(ix, insert):
    print('\n\ttimeplus-connect Python Insert (row oriented):')
    data = tc_client.query(insert['query']).result_rows
    table = f'perf_test_insert_{run_id}_{ix}'
    with test_ctx(table, insert) as ctx:
        start = time.time()
        tc_client.insert(table, data, ctx.column_names, column_type_names=ctx.column_types)
    _print_result(start, len(data))


def dr_write_python_columns(ix, insert):
    print('\n\tproton-driver Python Insert (column oriented):')
    data = pd_client.execute(insert['query'], columnar=True)
    table = f'perf_test_insert_{run_id}_{ix}'
    with test_ctx(table, insert) as ctx:
        cols = ','.join(ctx.column_names)
        start = time.time()
        pd_client.execute(f'INSERT INTO {table} ({cols}) VALUES', data, columnar=True)
    _print_result(start, len(data[0]))


def dr_write_python_rows(ix, insert):
    print('\n\tproton-driver Python Insert (row oriented):')
    data = pd_client.execute(insert['query'], columnar=False)
    table = f'perf_test_insert_{run_id}_{ix}'
    with test_ctx(table, insert) as ctx:
        cols = ','.join(ctx.column_names)
        start = time.time()
        pd_client.execute(f'INSERT INTO {table} ({cols}) VALUES', data, columnar=False)
    _print_result(start, len(data))


def test_ctx(table, insert):
    return TableContext(tc_client, table, insert['columns'])


def _print_result(start, rows):
    total_time = time.time() - start
    print(f'\t\tTime: {total_time:.4f} sec  rows: {rows}  rows/sec {rows // total_time}')


def main():
    for ix, insert in enumerate(inserts):
        if ix in excluded:
            continue
        print(f"\n{insert['query']}")
        # write_python_columns(ix, insert)
        write_python_rows(ix, insert)
        # dr_write_python_columns(ix, insert)
        dr_write_python_rows(ix, insert)


class CDWrapper:
    def __init__(self, client):
        self._client = client

    def command(self, cmd):
        self._client.execute(cmd)


if __name__ == '__main__':
    main()
