import pytest
from timeplus_connect.driver.exceptions import ProgrammingError

from timeplus_connect.datatypes.registry import get_from_name
from tests.helpers import to_bytes, native_insert_block
from tests.unit_tests.test_driver.binary import NESTED_BINARY

LOW_CARD_OUTPUT = """
0101 0576 616c 7565 204c 6f77 4361 7264
696e 616c 6974 7928 4e75 6c6c 6162 6c65
2853 7472 696e 6729 2901 0000 0000 0000
0000 0600 0000 0000 0002 0000 0000 0000
0000 0574 6872 6565 0100 0000 0000 0000
01
"""

TUPLE_ONE_OUTPUT = """
0101 0576 616c 7565 3854 7570 6c65 2853 
7472 696e 672c 2046 6c6f 6174 3332 2c20  
4c6f 7743 6172 6469 6e61 6c69 7479 284e  
756c 6c61 626c 6528 5374 7269 6e67 2929 
2901 0000 0000 0000 0007 7374 7269 6e67 
317b 144e 4000 0600 0000 0000 0001 0000  
0000 0000 0000 0100 0000 0000 0000 00  
"""

TUPLE_THREE_OUTPUT = """
0103 0576 616c 7565 0d54 7570 6c65 2853
7472 696e 6729 0773 7472 696e 6731 0773
7472 696e 6732 0773 7472 696e 6733
"""

POINT_OUTPUT = """
0101 0576 616c 7565 0550 6f69 6e74 c3f5
285c 8fc2 0940 c3f5 285c 8fc2 0940
"""

STRING_ACCEPTS_BYTES_OUTPUT = """
0101 0576 616c 7565 0653 7472 696e 6701
ff
"""

MAP_LOW_CARDINALITY_OUTPUT = """
0102 034D 4150 234D 6170 284C 6F77 4361
7264 696E 616C 6974 7928 5374 7269 6E67
292C 2053 7472 696E 6729 0100 0000 0000
0000 0200 0000 0000 0000 0200 0000 0000
0000 0006 0000 0000 0000 0200 0000 0000
0000 046B 6579 3104 6B65 7932 0200 0000
0000 0000 0001 0131 0374 776F
"""

LOW_CARDINALITY_NULLABLE_OUTPUT = """
0102 0373 7472 204C 6F77 4361 7264 696E
616C 6974 7928 4E75 6C6C 6162 6C65 2853
7472 696E 6729 2901 0000 0000 0000 0000
0600 0000 0000 0002 0000 0000 0000 0000
0566 6972 7374 0200 0000 0000 0000 0100
"""


def test_low_card_null():
    pytest.skip("binary data has not been modified yet")
    data = [['three']]
    names = ['value']
    types = [get_from_name('low_cardinality(nullable(string))')]
    output = native_insert_block(data, names, types)
    assert bytes(output) == to_bytes(LOW_CARD_OUTPUT)


def test_tuple_one():
    pytest.skip("binary data has not been modified yet")
    data = [[('string1', 3.22, None)]]
    names = ['value']
    types = [get_from_name('tuple(string, float32, low_cardinality(nullable(string)))')]
    output = native_insert_block(data, names, types)
    assert bytes(output) == bytes.fromhex(TUPLE_ONE_OUTPUT)


def test_tuple_three():
    pytest.skip("binary data has not been modified yet")
    data = [[('string1',)], [('string2',)], [('string3',)]]
    names = ['value']
    types = [get_from_name('tuple(string)')]
    output = native_insert_block(data, names, types)
    assert bytes(output) == bytes.fromhex(TUPLE_THREE_OUTPUT)


def test_point():
    pytest.skip("binary data has not been modified yet")
    data = [[(3.22, 3.22)]]
    names = ['value']
    types = [get_from_name('Point')]
    output = native_insert_block(data, names, types)
    assert bytes(output) == bytes.fromhex(POINT_OUTPUT)


def test_nested():
    pytest.skip("binary data has not been modified yet")
    data = [([],),
            ([{'str1': 'three', 'int32': 5}, {'str1': 'five', 'int32': 77}],),
            ([{'str1': 'one', 'int32': 5}, {'str1': 'two', 'int32': 55}],),
            ([{'str1': 'one', 'int32': 5}, {'str1': 'two', 'int32': 55}],)]
    types = [get_from_name('nested(str1 string, int32 uint32)')]
    output = native_insert_block(data, ['nested'], types)
    assert bytes(output) == bytes.fromhex(NESTED_BINARY)


def test_string_accepts_bytes():
    pytest.skip("binary data has not been modified yet")
    data = [[bytes.fromhex('ff')]]
    names = ['value']
    types = [get_from_name('string')]
    output = native_insert_block(data, names, types)
    assert bytes(output) == bytes.fromhex(STRING_ACCEPTS_BYTES_OUTPUT)


def test_long_str():
    pytest.skip("binary data has not been modified yet")
    x = ('蹝ㅝǅ잍鞏≈ﬞ㉢嫩杻⤧㛕錍к❭䦳텶샖爤㍅䱃䰅ἐ䤖엋㰾멛蹒뀃쩷섡፳聣᮵峧쒝咋觀હ鷁䯕͢퐠㏈猡칆빃밥뜼৫葘鹯勲掾ᬗ罧炼䏦險ヤⴕ懺릨봟죩ᬨ칰ԁ凢' +
         '䰚娞祃獿휢듕鞜甲뉛⠆ᗫ䐼詠圂ᱞ出裒ਗ਼ᩜ㉤扷ꑐ晏镄焬㞧ノⷶ枆侪㇉摨⒞펦埏穊僛䦃吹ꗣ麥䔲鸈麡┨࣓ꢫႮﬆᝢ妢曢ꗠᆪ擽烣졀씥⣏便꽉슕盈㪃拪풻ᯖ럐峨' +
         '箻躰䆲⏂錬횬渪㜟첯鋘ꊩ㾝톶╁茒牾붮뚂О灪噚놾蠂쌇龥䁼')
    data = [[x]]
    names = ['value']
    types = [get_from_name('string')]
    output = native_insert_block(data, names, types)
    assert bytes(output) == b'\x01\x01\x05value\x06string\xe7\x03' + x.encode()


def test_low_card_map():
    pytest.skip("proton does not support geometric type")
    data = [[{'key1': '1', 'key2': 'two'}], [{}]]
    names = ['MAP']
    types = [get_from_name('map(low_cardinality(string), string)')]
    output = native_insert_block(data, names, types)
    assert bytes(output) == bytes.fromhex(MAP_LOW_CARDINALITY_OUTPUT)


def test_low_card_nullable():
    pytest.skip("proton does not support geometric type")
    data = [['first'], [None]]
    names = ['str']
    types = [get_from_name('low_cardinality(nullable(string))')]
    output = native_insert_block(data, names, types)
    assert bytes(output) == bytes.fromhex(LOW_CARDINALITY_NULLABLE_OUTPUT)


def test_bad_columns():
    pytest.skip("proton does not support geometric type")
    data = [['str'], [3.5]]
    names = ['value']
    types = [get_from_name('string')]
    try:
        native_insert_block(data, names, types)
    except TypeError:
        pass

    data = [[3.5], [str]]
    names = ['value']
    types = [get_from_name('float64')]

    try:
        native_insert_block(data, names, types)
    except ProgrammingError:
        pass
