# pylint: disable=protected-access
from clickhouse_connect.datatypes.container import Nested
from clickhouse_connect.datatypes.registry import get_from_name as gfn


def test_enum_parse():
    enum_type = gfn("enum8('OZC|8;' = -125, '6MQ4v-t' = -114, 'As7]sEg\\'' = 40, 'v~l$PR5' = 84)")
    assert 'OZC|8;' in enum_type._name_map
    enum_type = gfn('enum8(\'\\\'"2Af\' = 93,\'KG;+\\\' = -114,\'j0\' = -40)')
    assert '\'"2Af' in enum_type._name_map
    enum_type = gfn("enum8('value1' = 7, 'value2'=5)")
    assert enum_type.name == "enum8('value2' = 5, 'value1' = 7)"
    assert 7 in enum_type._int_map
    assert 5 in enum_type._int_map
    enum_type = gfn(r"enum16('beta&&' = -3, '' = 0, 'alpha\'' = 3822)")
    assert r"alpha'" == enum_type._int_map[3822]
    assert -3 == enum_type._name_map['beta&&']


def test_names():
    array_type = gfn('array(nullable(fixed_string(50)))')
    assert array_type.name == 'array(nullable(fixed_string(50)))'
    array_type = gfn(
        "array(enum8(\'user_name\' = 1, \'ip_address\' = -2, \'forwarded_ip_address\' = 3, \'client_key\' = 4))")
    assert array_type.name == (
        "array(enum8('ip_address' = -2, 'user_name' = 1, 'forwarded_ip_address' = 3, 'client_key' = 4))")


def test_nested_parse():
    nested_type = gfn('nested(str1 string, int32 uint32)')
    assert nested_type.name == 'nested(str1 string, int32 uint32)'
    assert isinstance(nested_type, Nested)
    nested_type = gfn('nested(id int64, data nested(inner_key string, inner_map map(string, )))')
    assert nested_type.name == 'nested(id int64, data nested(inner_key string, inner_map map(string, )))'
    nest = "key_0 enum16('[m(X*' = -18773, '_9as' = 11854, '&e$LE' = 27685), key_1 nullable(Decimal(62, 38))"
    nested_name = f'nested({nest})'
    nested_type = gfn(nested_name)
    assert nested_type.name == nested_name


def test_named_tuple():
    tuple_type = gfn('tuple(int64, string)')
    assert tuple_type.name == 'tuple(int64, string)'
    tuple_type = gfn('tuple(`key` int64, `value` string)')
    assert tuple_type.name == 'tuple(`key` int64, `value` string)'
