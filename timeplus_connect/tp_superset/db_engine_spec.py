import logging
from datetime import datetime
from typing import Any

# pylint:disable=E0401, E0611
from marshmallow import fields, Schema
from marshmallow.validate import Range
from superset.db_engine_specs.base import BaseEngineSpec
from superset.utils import core as utils
from superset.sql.parse import SQLGLOT_DIALECTS

from sqlalchemy import types
from sqlalchemy.engine.url import URL


from timeplus_connect.tp_sqlglot.dialect import TimeplusSqlglotDialect


logger = logging.getLogger(__name__)

try:
    from timeplus_connect.datatypes.format import set_default_formats

    # override default formats for compatibility
    set_default_formats(
        "fixed_string",
        "string",
        "ipv*",
        "string",
        "uint64",
        "signed",
        "UUID",
        "string",
        "*int256",
        "string",
        "*int128",
        "string",
    )
except ImportError:  # Timeplus Connect not installed, do nothing
    pass

class TimeplusParametersSchema(Schema):
    username = fields.String(allow_none=True, metadata={"description": utils.__("Username")})
    password = fields.String(allow_none=True, metadata={"description": utils.__("Password")})
    host = fields.String(
        required=True, metadata={"description": utils.__("Hostname or IP address")}
    )
    port = fields.Integer(
        allow_none=True,
        metadata={"description": utils.__("Database port")},
        validate=Range(min=0, max=65535),
    )
    database = fields.String(
        allow_none=True, metadata={"description": utils.__("Database name")}
    )
    encryption = fields.Boolean(
        dump_default=True,
        metadata={"description": utils.__("Use an encrypted connection to the database")},
    )
    query = fields.Dict(
        keys=fields.Str(),
        values=fields.Raw(),
        metadata={"description": utils.__("Additional parameters")},
    )


class TimeplusEngineSpec(BaseEngineSpec):
    """Engine spec for timeplus-connect connector"""

    engine = "timeplus"
    engine_name = "Timeplus Connect (Superset)"

    _show_functions_column = "name"
    supports_file_upload = False

    sqlalchemy_uri_placeholder = (
        "timeplus://user:password@host[:port][/dbname][?secure=value&=value...]"
    )

    SQLGLOT_DIALECTS["timeplus"] = TimeplusSqlglotDialect

    parameters_schema = TimeplusParametersSchema()
    encryption_parameters = {"secure": "true"}

    _time_grain_expressions = {
        None: "{col}",
        "PT1M": "to_start_of_minute(to_datetime({col}))",
        "PT5M": "to_datetime(cast(to_datetime({col}) as int32)/300*300)",
        "PT10M": "to_datetime(cast(to_datetime({col}) as int32)/600*600)",
        "PT15M": "to_datetime(cast(to_datetime({col}) as int32)/900*900)",
        "PT30M": "to_datetime(cast(to_datetime({col}) as int32)/1800*1800)",
        "PT1H": "to_start_of_hour(to_datetime({col}))",
        "P1D": "to_start_of_day(to_datetime({col}))",
        "P1M": "to_start_of_month(to_datetime({col}))",
        "P3M": "to_start_of_quarter(to_datetime({col}))",
        "P1Y": "to_start_of_year(to_datetime({col}))",
    }

    @classmethod
    def epoch_to_dttm(cls) -> str:
        return "{col}"

    @classmethod
    def convert_dttm(
        cls, target_type: str, dttm: datetime, db_extra: dict[str, Any] | None = None # pylint: disable=W0613
    ) -> str | None:
        sqla_type = cls.get_sqla_column_type(target_type)

        if isinstance(sqla_type, types.Date):
            return f"to_date('{dttm.date().isoformat()}')"
        if isinstance(sqla_type, types.DateTime):
            return f"""to_datetime('{dttm.isoformat(sep=" ", timespec="seconds")}')"""
        return None

    @classmethod
    def get_datatype(cls, type_code: str) -> str:
        # keep it lowercase, as Timeplus types aren't typical SHOUTCASE ANSI SQL
        return type_code

    @classmethod
    def build_sqlalchemy_uri(
        cls,
        parameters: dict[str, str],
        encrypted_extra: dict[str, str] | None = None, # pylint: disable=W0613
    ) -> str:
        url_params = parameters.copy()
        if url_params.get("encryption"):
            query = parameters.get("query", {}).copy()
            query.update(cls.encryption_parameters)
            url_params["query"] = query
        if not url_params.get("database"):
            url_params["database"] = "default"
        url_params.pop("encryption", None)
        return str(URL.create(f"{cls.engine}", **url_params))
