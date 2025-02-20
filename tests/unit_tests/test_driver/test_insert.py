import datetime

from timeplus_connect.datatypes.registry import get_from_name

from timeplus_connect.driver.insert import InsertContext
from timeplus_connect.tools.datagen import fixed_len_ascii_str


def test_block_size():
    data = [(1, (datetime.date(2020, 5, 2), datetime.datetime(2020, 5, 2, 10, 5, 2)))]
    ctx = InsertContext('fake_table',
                        ['key', 'date_tuple'],
                        [get_from_name('uint64'), get_from_name('tuple(Date, DateTime)')],
                        data)
    assert ctx.block_row_count == 262144

    data = [(x, fixed_len_ascii_str(400)) for x in range(5000)]
    ctx = InsertContext('fake_table',
                        ['key', 'big_str'],
                        [get_from_name('int32'), get_from_name('string')],
                        data)
    assert ctx.block_row_count == 8192
