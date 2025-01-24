import logging

from typing import Tuple, Dict
from timeplus_connect.datatypes.base import TypeDef, TimeplusType, type_map
from timeplus_connect.driver.exceptions import InternalError
from timeplus_connect.driver.parser import parse_enum, parse_callable, parse_columns

logger = logging.getLogger(__name__)
type_cache: Dict[str, TimeplusType] = {}


def parse_name(name: str) -> Tuple[str, str, TypeDef]:
    """
    Converts a Timeplus type name into the base class and the definition (TypeDef) needed for any
    additional instantiation
    :param name: Timeplus type name as returned by timeplusd
    :return: The original base name (before arguments), the full name as passed in and the TypeDef object that
     captures any additional arguments
    """
    base = name
    wrappers = []
    keys = tuple()
    if base.startswith('low_cardinality'):
        wrappers.append('low_cardinality')
        base = base[16:-1]
    if base.startswith('nullable'):
        wrappers.append('nullable')
        base = base[9:-1]
    if base.startswith('enum'):
        keys, values = parse_enum(base)
        base = base[:base.find('(')]
    elif base.startswith('nested'):
        keys, values = parse_columns(base[6:])
        base = 'nested'
    elif base.startswith('tuple'):
        keys, values = parse_columns(base[5:])
        base = 'tuple'
    elif base.startswith('variant'):
        keys, values = parse_columns(base[7:])
        base = 'variant'
    elif base.startswith('JSON') and len(base) > 4 and base[4] == '(':
        keys, values = parse_columns(base[4:])
        base = 'JSON'
    # timeplusd doesn't support geometric type.
    # elif base == 'Point':
    #     values = ('Float64', 'Float64')
    else:
        try:
            base, values, _ = parse_callable(base)
        except IndexError:
            raise InternalError(f'Can not parse Timeplus data type: {name}') from None
    return base, name, TypeDef(tuple(wrappers), keys, values)


def get_from_name(name: str) -> TimeplusType:
    """
    Returns the TimeplusType instance parsed from the Timeplus type name.  Instances are cached
    :param name: Timeplus type name as returned by Timeplus in WithNamesAndTypes FORMAT or the Native protocol
    :return: The instance of the Timeplus Type
    """
    ch_type = type_cache.get(name, None)
    if not ch_type:
        base, name, type_def = parse_name(name)
        try:
            ch_type = type_map[base].build(type_def)
        except KeyError:
            err_str = f'Unrecognized Timeplus type base: {base} name: {name}'
            logger.error(err_str)
            raise InternalError(err_str) from None
        type_cache[name] = ch_type
    return ch_type
