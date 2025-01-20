from typing import Union, Sequence, MutableSequence, Any
from uuid import UUID as PYUUID

from timeplus_connect.datatypes.base import TypeDef, TimeplusType, ArrayType, UnsupportedType
from timeplus_connect.datatypes.registry import get_from_name
from timeplus_connect.driver.common import first_value
from timeplus_connect.driver.ctypes import data_conv
from timeplus_connect.driver.insert import InsertContext
from timeplus_connect.driver.query import QueryContext
from timeplus_connect.driver.types import ByteSource

empty_uuid_b = bytes(b'\x00' * 16)


class UUID(TimeplusType):
    valid_formats = 'string', 'native'
    np_type = 'U36'
    byte_size = 16
    base_type = ('uuid', )

    def python_null(self, ctx):
        return '' if self.read_format(ctx) == 'string' else PYUUID(int=0)

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext, _read_state: Any):
        if self.read_format(ctx) == 'string':
            return self._read_binary_str(source, num_rows)
        return data_conv.read_uuid_col(source, num_rows)

    @staticmethod
    def _read_binary_str(source: ByteSource, num_rows: int):
        v = source.read_array('Q', num_rows * 2)
        column = []
        app = column.append
        for i in range(num_rows):
            ix = i << 1
            x = f'{(v[ix] << 64 | v[ix + 1]):032x}'
            app(f'{x[:8]}-{x[8:12]}-{x[12:16]}-{x[16:20]}-{x[20:]}')
        return column

    # pylint: disable=too-many-branches
    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: bytearray, ctx: InsertContext):
        first = first_value(column, self.nullable)
        empty = empty_uuid_b
        if isinstance(first, str) or self.write_format(ctx) == 'string':
            for v in column:
                if v:
                    x = int(v.replace('-', ''), 16)
                    dest += (x >> 64).to_bytes(8, 'little') + (x & 0xffffffffffffffff).to_bytes(8, 'little')
                else:
                    dest += empty
        elif isinstance(first, int):
            for x in column:
                if x:
                    dest += (x >> 64).to_bytes(8, 'little') + (x & 0xffffffffffffffff).to_bytes(8, 'little')
                else:
                    dest += empty
        elif isinstance(first, PYUUID):
            for v in column:
                if v:
                    x = v.int
                    dest += (x >> 64).to_bytes(8, 'little') + (x & 0xffffffffffffffff).to_bytes(8, 'little')
                else:
                    dest += empty
        elif isinstance(first, (bytes, bytearray, memoryview)):
            for v in column:
                if v:
                    dest += bytes(reversed(v[:8])) + bytes(reversed(v[8:]))
                else:
                    dest += empty
        else:
            dest += empty * len(column)


class Nothing(ArrayType):
    _array_type = 'b'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.nullable = True

    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: bytearray, _ctx):
        dest += bytes(0x30 for _ in range(len(column)))


class SimpleAggregateFunction(TimeplusType):
    _slots = ('element_type',)
    base_type = ('simple_aggregate_function', )

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.element_type: TimeplusType = get_from_name(type_def.values[1])
        self._name_suffix = type_def.arg_str
        self.byte_size = self.element_type.byte_size

    def _data_size(self, sample: Sequence) -> int:
        return self.element_type.data_size(sample)

    def read_column_prefix(self, source: ByteSource, ctx: QueryContext):
        return self.element_type.read_column_prefix(source, ctx)

    def write_column_prefix(self, dest: bytearray):
        self.element_type.write_column_prefix(dest)

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext, read_state: Any):
        return self.element_type.read_column_data(source, num_rows, ctx, read_state)

    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: bytearray, ctx: InsertContext):
        self.element_type.write_column_data(column, dest, ctx)


class AggregateFunction(UnsupportedType):
    base_type = ('aggregate_function', )
