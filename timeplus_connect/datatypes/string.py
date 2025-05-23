from typing import Sequence, MutableSequence, Union, Collection, Any

from timeplus_connect.driver.common import first_value
from timeplus_connect.driver.ctypes import data_conv

from timeplus_connect.datatypes.base import TimeplusType, TypeDef
from timeplus_connect.driver.errors import handle_error
from timeplus_connect.driver.insert import InsertContext
from timeplus_connect.driver.query import QueryContext
from timeplus_connect.driver.types import ByteSource
from timeplus_connect.driver.options import np, pd


class String(TimeplusType):
    valid_formats = 'bytes', 'native'
    base_type = ('string', )

    def _active_encoding(self, ctx):
        if self.read_format(ctx) == 'bytes':
            return None
        if ctx.encoding:
            return ctx.encoding
        return self.encoding

    def _data_size(self, sample: Collection) -> int:
        if len(sample) == 0:
            return 0
        total = 0
        for x in sample:
            if x:
                total += len(x)
        return total // len(sample) + 1

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext, _read_state: Any):
        return source.read_str_col(num_rows, self._active_encoding(ctx))

    def _read_nullable_column(self, source: ByteSource, num_rows: int, ctx: QueryContext, read_state: Any) -> Sequence:
        return source.read_str_col(num_rows, self._active_encoding(ctx), True, self._active_null(ctx))

    def _finalize_column(self, column: Sequence, ctx: QueryContext) -> Sequence:
        if ctx.use_extended_dtypes and self.read_format(ctx) == 'native':
            return pd.array(column, dtype=pd.StringDtype())
        if ctx.use_numpy and ctx.max_str_len:
            return np.array(column, dtype=f'<U{ctx.max_str_len}')
        return column

    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: bytearray, ctx: InsertContext):
        encoding = None
        if not isinstance(first_value(column, self.nullable), bytes):
            encoding = ctx.encoding or self.encoding
        handle_error(data_conv.write_str_col(column, self.nullable, encoding, dest), ctx)

    def _active_null(self, ctx):
        if ctx.use_none:
            return None
        if self.read_format(ctx) == 'bytes':
            return bytes()
        return ''


class FixedString(TimeplusType):
    valid_formats = 'string', 'native'
    base_type = ('fixed_string', )

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.byte_size = type_def.values[0]
        self._name_suffix = type_def.arg_str
        self._empty_bytes = bytes(b'\x00' * self.byte_size)

    def _active_null(self, ctx: QueryContext):
        if ctx.use_none:
            return None
        return self._empty_bytes if self.read_format(ctx) == 'native' else ''

    @property
    def np_type(self):
        return f'<U{self.byte_size}'

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext, _read_state: Any):
        if self.read_format(ctx) == 'string':
            return source.read_fixed_str_col(self.byte_size, num_rows, ctx.encoding or self.encoding )
        return source.read_bytes_col(self.byte_size, num_rows)

    def _finalize_column(self, column: Sequence, ctx: QueryContext) -> Sequence:
        if ctx.use_extended_dtypes and self.read_format(ctx) == 'string':
            return pd.array(column, dtype=pd.StringDtype())
        return column

    # pylint: disable=too-many-branches,duplicate-code
    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: bytearray, ctx: InsertContext):
        ext = dest.extend
        sz = self.byte_size
        empty = bytes((0,) * sz)
        str_enc = str.encode
        enc = ctx.encoding or self.encoding
        first = first_value(column, self.nullable)
        if isinstance(first, str) or self.write_format(ctx) == 'string':
            if self.nullable:
                for x in column:
                    if x is None:
                        ext(empty)
                    else:
                        try:
                            b = str_enc(x, enc)
                        except UnicodeEncodeError:
                            b = empty
                        if len(b) > sz:
                            raise ctx.data_error(f'UTF-8 encoded fixed_string value {b.hex(" ")} exceeds column size {sz}')
                        ext(b)
                        ext(empty[:sz - len(b)])
            else:
                for x in column:
                    try:
                        b = str_enc(x, enc)
                    except UnicodeEncodeError:
                        b = empty
                    if len(b) > sz:
                        raise ctx.data_error(f'UTF-8 encoded fixed_string value {b.hex(" ")} exceeds column size {sz}')
                    ext(b)
                    ext(empty[:sz - len(b)])
        elif self.nullable:
            for b in column:
                if not b:
                    ext(empty)
                elif len(b) != sz:
                    raise ctx.data_error(f'Fixed String binary value {b.hex(" ")} does not match column size {sz}')
                else:
                    ext(b)
        else:
            for b in column:
                if len(b) != sz:
                    raise ctx.data_error(f'Fixed String binary value {b.hex(" ")} does not match column size {sz}')
                ext(b)
