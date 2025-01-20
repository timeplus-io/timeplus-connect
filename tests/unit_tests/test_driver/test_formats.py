from timeplus_connect.datatypes.format import set_default_formats, set_write_format
from timeplus_connect.datatypes.network import IPv6
from timeplus_connect.datatypes.numeric import Int32
from timeplus_connect.datatypes.string import FixedString
from timeplus_connect.driver.context import BaseQueryContext
from timeplus_connect.driver.query import QueryContext


def test_default_formats():
    ctx = QueryContext()
    set_default_formats('int32', 'string', 'IP*', 'string')
    assert IPv6.read_format(ctx) == 'string'
    assert Int32.read_format(ctx) == 'string'
    assert FixedString.read_format(ctx) == 'native'


def test_fixed_str_format():
    set_write_format('fixed_string', 'string')
    assert FixedString.write_format(BaseQueryContext()) == 'string'
