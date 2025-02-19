import pytz
from enum import Enum as PyEnum
from typing import Type, Union, Sequence

from sqlalchemy.types import Integer, Float, Numeric, Boolean as SqlaBoolean, \
    UserDefinedType, String as SqlaString, DateTime as SqlaDateTime, Date as SqlaDate
from sqlalchemy.exc import ArgumentError

from timeplus_connect.cc_sqlalchemy.datatypes.base import TpSqlaType
from timeplus_connect.datatypes.base import TypeDef, NULLABLE_TYPE_DEF, LC_TYPE_DEF, EMPTY_TYPE_DEF
from timeplus_connect.datatypes.numeric import Enum8 as ChEnum8, Enum16 as ChEnum16
from timeplus_connect.driver.common import decimal_prec


class Int8(TpSqlaType, Integer):
    base = ('int8', )


class UInt8(TpSqlaType, Integer):
    base = ('uint8', )


class Int16(TpSqlaType, Integer):
    base = ('int16', )


class UInt16(TpSqlaType, Integer):
    base = ('uint16', )


class Int32(TpSqlaType, Integer):
    base = ('int32', 'int')


class UInt32(TpSqlaType, Integer):
    base = ('uint32', 'uint')


class Int64(TpSqlaType, Integer):
    base = ('int64', )


class UInt64(TpSqlaType, Integer):
    base = ('uint64', )


class Int128(TpSqlaType, Integer):
    base = ('int128', )


class UInt128(TpSqlaType, Integer):
    base = ('uint128', )


class Int256(TpSqlaType, Integer):
    base = ('int256', )


class UInt256(TpSqlaType, Integer):
    base = ('uint256', )


class Float32(TpSqlaType, Float):
    base = ('float32', )
    def __init__(self, type_def: TypeDef = EMPTY_TYPE_DEF):
        TpSqlaType.__init__(self, type_def)
        Float.__init__(self)


class Float64(TpSqlaType, Float):
    base = ('float64', )
    def __init__(self, type_def: TypeDef = EMPTY_TYPE_DEF):
        TpSqlaType.__init__(self, type_def)
        Float.__init__(self)


class Bool(TpSqlaType, SqlaBoolean):
    base = ('bool', )
    def __init__(self, type_def: TypeDef = EMPTY_TYPE_DEF):
        TpSqlaType.__init__(self, type_def)
        SqlaBoolean.__init__(self)


class Boolean(Bool):
    pass


class Decimal(TpSqlaType, Numeric):
    base = ('Decimal', 'decimal')
    dec_size = 0

    def __init__(self, precision: int = 0, scale: int = 0, type_def: TypeDef = None):
        """
        Construct either with precision and scale (for DDL), or a TypeDef with those values (by name)
        :param precision:  Number of digits the Decimal
        :param scale: Digits after the decimal point
        :param type_def: Parsed type def from ClickHouse arguments
        """
        if type_def:
            if self.dec_size:
                precision = decimal_prec[self.dec_size]
                scale = type_def.values[0]
            else:
                precision, scale = type_def.values
        elif not precision or scale < 0 or scale > precision:
            raise ArgumentError('Invalid precision or scale for ClickHouse Decimal type')
        else:
            type_def = TypeDef(values=(precision, scale))
        TpSqlaType.__init__(self, type_def)
        Numeric.__init__(self, precision, scale)


# pylint: disable=duplicate-code
class Decimal32(Decimal):
    base = ('Decimal32', 'decimal32')
    dec_size = 32


class Decimal64(Decimal):
    base = ('Decimal64', 'decimal64')
    dec_size = 64


class Decimal128(Decimal):
    base = ('Decimal128', 'decimal128')
    dec_size = 128


class Decimal256(Decimal):
    base = ('Decimal256', 'decimal256')
    dec_size = 256


class Enum(TpSqlaType, UserDefinedType):
    _size = 16
    python_type = str
    base = ('enum', )

    def __init__(self, enum: Type[PyEnum] = None, keys: Sequence[str] = None, values: Sequence[int] = None,
                 type_def: TypeDef = None):
        """
        Construct a ClickHouse enum either from a Python Enum or parallel lists of keys and value.  Note that
        Python enums do not support empty strings as keys, so the alternate keys/values must be used in that case
        :param enum: Python enum to convert
        :param keys: List of string keys
        :param values: List of integer values
        :param type_def: TypeDef from parse_name function
        """
        if not type_def:
            if enum:
                keys = [e.name for e in enum]
                values = [e.value for e in enum]
            self._validate(keys, values)
            if self.__class__.__name__ == 'Enum':
                if max(values) <= 127 and min(values) >= -128:
                    self._ch_type_cls = ChEnum8
                else:
                    self._ch_type_cls = ChEnum16
            type_def = TypeDef(keys=tuple(keys), values=tuple(values))
        super().__init__(type_def)

    @classmethod
    def _validate(cls, keys: Sequence, values: Sequence):
        bad_key = next((x for x in keys if not isinstance(x, str)), None)
        if bad_key:
            raise ArgumentError(f'ClickHouse enum key {bad_key} is not a string')
        bad_value = next((x for x in values if not isinstance(x, int)), None)
        if bad_value:
            raise ArgumentError(f'ClickHouse enum value {bad_value} is not an integer')
        value_min = -(2 ** (cls._size - 1))
        value_max = 2 ** (cls._size - 1) - 1
        bad_value = next((x for x in values if x < value_min or x > value_max), None)
        if bad_value:
            raise ArgumentError(f'Timeplus enum value {bad_value} is out of range')


class Enum8(Enum):
    _size = 8
    _ch_type_cls = ChEnum8
    base = ('enum8', )


class Enum16(Enum):
    _ch_type_cls = ChEnum16
    base = ('enum16', )


class String(TpSqlaType, UserDefinedType):
    python_type = str
    base = ('string', )


class FixedString(TpSqlaType, SqlaString):
    base = ('fixed_string', )
    def __init__(self, size: int = -1, type_def: TypeDef = None):
        if not type_def:
            type_def = TypeDef(values=(size,))
        TpSqlaType.__init__(self, type_def)
        SqlaString.__init__(self, size)


class IPv4(TpSqlaType, UserDefinedType):
    python_type = None
    base = ('ipv4', )


class IPv6(TpSqlaType, UserDefinedType):
    python_type = None
    base = ('ipv6', )


class UUID(TpSqlaType, UserDefinedType):
    python_type = None
    base = ('uuid', )


class Nothing(TpSqlaType, UserDefinedType):
    python_type = None


# proton doesn't support geometric type
class Point(TpSqlaType, UserDefinedType):
    python_type = None


class Ring(TpSqlaType, UserDefinedType):
    python_type = None


class Polygon(TpSqlaType, UserDefinedType):
    python_type = None


class MultiPolygon(TpSqlaType, UserDefinedType):
    python_type = None


class LineString(TpSqlaType, UserDefinedType):
    python_type = None


class MultiLineString(TpSqlaType, UserDefinedType):
    python_type = None


class Date(TpSqlaType, SqlaDate):
    base = ('date', 'Date')


class Date32(TpSqlaType, SqlaDate):
    base = ('date32', 'Date32')


class DateTime(TpSqlaType, SqlaDateTime):
    base = ('datetime', 'DateTime')
    def __init__(self, tz: str = None, type_def: TypeDef = None):
        """
        Date time constructor with optional ClickHouse timezone parameter if not constructed with TypeDef
        :param tz: Timezone string as defined in pytz
        :param type_def: TypeDef from parse_name function
        """
        if not type_def:
            if tz:
                pytz.timezone(tz)
                type_def = TypeDef(values=(f"'{tz}'",))
            else:
                type_def = EMPTY_TYPE_DEF
        TpSqlaType.__init__(self, type_def)
        SqlaDateTime.__init__(self)


class DateTime64(TpSqlaType, SqlaDateTime):
    base = ('datetime64', 'DateTime64')
    def __init__(self, precision: int = None, tz: str = None, type_def: TypeDef = None):
        """
        Date time constructor with precision and timezone parameters if not constructed with TypeDef
        :param precision:   Usually 3/6/9 for mill/micro/nanosecond precision on ClickHouse side
        :param tz: Timezone string as defined in pytz
        :param type_def: TypeDef from parse_name function
        """
        if not type_def:
            if tz:
                pytz.timezone(tz)
                type_def = TypeDef(values=(precision, f"'{tz}'"))
            else:
                type_def = TypeDef(values=(precision,))
        prec = type_def.values[0] if len(type_def.values) else None
        if not isinstance(prec, int) or prec < 0 or prec > 9:
            raise ArgumentError(f'Invalid precision value {prec} for ClickHouse DateTime64')
        TpSqlaType.__init__(self, type_def)
        SqlaDateTime.__init__(self)


class Nullable:
    """
    Class "wrapper" to use in DDL construction.  It is never actually initialized but instead creates the "wrapped"
    type with a Nullable wrapper
    """
    base = ('nullable', )

    def __new__(cls, element: Union[TpSqlaType, Type[TpSqlaType]]):
        """
        Actually returns an instance of the enclosed type with a Nullable wrapper.  If element is an instance,
        constructs a new instance with a copied TypeDef plus the Nullable wrapper.  If element is just a type,
        constructs a new element of that type with only the Nullable wrapper.
        :param element: TpSqlaType instance or class to wrap
        """
        if callable(element):
            return element(type_def=NULLABLE_TYPE_DEF)
        if element.low_card:
            raise ArgumentError('Low Cardinality type cannot be nullable')
        orig = element.type_def
        wrappers = orig if 'nullable' in orig.wrappers else orig.wrappers + ('nullable',)
        return element.__class__(type_def=TypeDef(wrappers, orig.keys, orig.values))


class LowCardinality:
    """
    Class "wrapper" to use in DDL construction.  It is never actually instantiated but instead creates the "wrapped"
    type with a LowCardinality wrapper
    """
    base = ('low_cardinality', )

    def __new__(cls, element: Union[TpSqlaType, Type[TpSqlaType]]):
        """
       Actually returns an instance of the enclosed type with a LowCardinality wrapper.  If element is an instance,
       constructs a new instance with a copied TypeDef plus the LowCardinality wrapper.  If element is just a type,
       constructs a new element of that type with only the LowCardinality wrapper.
       :param element: TpSqlaType instance or class to wrap
       """
        if callable(element):
            return element(type_def=LC_TYPE_DEF)
        orig = element.type_def
        wrappers = orig if 'low_cardinality' in orig.wrappers else ('low_cardinality',) + orig.wrappers
        return element.__class__(type_def=TypeDef(wrappers, orig.keys, orig.values))


class Array(TpSqlaType, UserDefinedType):
    python_type = list
    base = ('array', )

    def __init__(self, element: Union[TpSqlaType, Type[TpSqlaType]] = None, type_def: TypeDef = None):
        """
        Array constructor that can take a wrapped Array type if not constructed from a TypeDef
        :param element: TpSqlaType instance or class to wrap
        :param type_def: TypeDef from parse_name function
        """
        if not type_def:
            if callable(element):
                element = element()
            type_def = TypeDef(values=(element.name,))
        super().__init__(type_def)


class Map(TpSqlaType, UserDefinedType):
    python_type = dict
    base = ('map', )

    def __init__(self, key_type: Union[TpSqlaType, Type[TpSqlaType]] = None,
                 value_type: Union[TpSqlaType, Type[TpSqlaType]] = None, type_def: TypeDef = None):
        """
        Map constructor that can take a wrapped key/values types if not constructed from a TypeDef
        :param key_type: TpSqlaType instance or class to use as keys for the Map
        :param value_type: TpSqlaType instance or class to use as values for the Map
        :param type_def: TypeDef from parse_name function
        """
        if not type_def:
            if callable(key_type):
                key_type = key_type()
            if callable(value_type):
                value_type = value_type()
            type_def = TypeDef(values=(key_type.name, value_type.name))
        super().__init__(type_def)


class Tuple(TpSqlaType, UserDefinedType):
    python_type = tuple
    base = ('tuple', )

    def __init__(self, elements: Sequence[Union[TpSqlaType, Type[TpSqlaType]]] = None, type_def: TypeDef = None):
        """
       Tuple constructor that can take a list of element types if not constructed from a TypeDef
       :param elements: sequence of TpSqlaType instance or class to use as tuple element types
       :param type_def: TypeDef from parse_name function
       """
        if not type_def:
            values = [et() if callable(et) else et for et in elements]
            type_def = TypeDef(values=tuple(v.name for v in values))
        super().__init__(type_def)


class JSON(TpSqlaType, UserDefinedType):
    """
    Note this isn't currently supported for insert/select, only table definitions
    """
    python_type = None
    base = ('json', )


class Nested(TpSqlaType, UserDefinedType):
    """
    Note this isn't currently supported for insert/select, only table definitions
    """
    python_type = None
    base = ('nested', )


class Object(TpSqlaType, UserDefinedType):
    """
    Note this isn't currently supported for insert/select, only table definitions
    """
    python_type = None
    base = ('object', )

    def __init__(self, fmt: str = None, type_def: TypeDef = None):
        if not type_def:
            type_def = TypeDef(values=(fmt,))
        super().__init__(type_def)


class SimpleAggregateFunction(TpSqlaType, UserDefinedType):
    python_type = None
    base = ('simple_aggregate_function', )

    def __init__(self, name: str = None, element: Union[TpSqlaType, Type[TpSqlaType]] = None, type_def: TypeDef = None):
        """
        Constructor that can take the SimpleAggregateFunction name and wrapped type if not constructed from a TypeDef
        :param name: Aggregate function name
        :param element: TpSqlaType instance or class which the function aggregates
        :param type_def: TypeDef from parse_name function
        """
        if not type_def:
            if callable(element):
                element = element()
            type_def = TypeDef(values=(name, element.name,))
        super().__init__(type_def)


class AggregateFunction(TpSqlaType, UserDefinedType):
    """
    Note this isn't currently supported for insert/select, only table definitions
    """
    python_type = None
    base = ('aggregate_function', )

    def __init__(self, *params, type_def: TypeDef = None):
        """
        Simply wraps the parameters for AggregateFunction for DDL, unless the TypeDef is specified.
        Callables or actual types are converted to their names.
        :param params: AggregateFunction parameters
        :param type_def: TypeDef from parse_name function
        """
        if not type_def:
            values = ()
            for x in params:
                if callable(x):
                    x = x()
                if isinstance(x, TpSqlaType):
                    x = x.name
                values += (x,)
            type_def = TypeDef(values=values)
        super().__init__(type_def)
