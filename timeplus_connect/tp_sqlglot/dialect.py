# pylint: disable=too-many-lines
from __future__ import annotations
import typing as t
import datetime
import logging

# pylint: disable=import-error
from sqlglot import exp, generator, parser, tokens
from sqlglot.dialects.dialect import (
    Dialect,
    NormalizationStrategy,
    arg_max_or_min_no_count,
    build_date_delta,
    build_formatted_time,
    inline_array_sql,
    json_extract_segments,
    json_path_key_only_name,
    no_pivot_sql,
    build_json_extract_path,
    rename_func,
    sha256_sql,
    var_map_sql,
    timestamptrunc_sql,
    unit_to_var,
    trim_sql,
)
from sqlglot.generator import Generator, unsupported_args
from sqlglot.helper import is_int, seq_get
from sqlglot.tokens import Token, TokenType

DatetimeDelta = t.Union[exp.DateAdd, exp.DateDiff, exp.DateSub, exp.TimestampSub, exp.TimestampAdd]

logger = logging.getLogger(__name__)

def _build_date_format(args: t.List) -> exp.TimeToStr:
    expr = build_formatted_time(exp.TimeToStr, "Timeplus")(args)

    timezone = seq_get(args, 2)
    if timezone:
        expr.set("zone", timezone)

    return expr


def _unix_to_time_sql(self: TimeplusSqlglotDialect.Generator, expression: exp.UnixToTime) -> str:
    scale = expression.args.get("scale")
    timestamp = expression.this

    if scale in (None, exp.UnixToTime.SECONDS):
        return self.func("from_unix_timestamp", exp.cast(timestamp, exp.DataType.Type.BIGINT))
    if scale == exp.UnixToTime.MILLIS:
        return self.func("from_unix_timestamp64_milli", exp.cast(timestamp, exp.DataType.Type.BIGINT))
    if scale == exp.UnixToTime.MICROS:
        return self.func("from_unix_timestamp64_micro", exp.cast(timestamp, exp.DataType.Type.BIGINT))
    if scale == exp.UnixToTime.NANOS:
        return self.func("from_unix_timestamp64_nano", exp.cast(timestamp, exp.DataType.Type.BIGINT))

    return self.func(
        "from_unix_timestamp",
        exp.cast(
            exp.Div(this=timestamp, expression=exp.func("POW", 10, scale)), exp.DataType.Type.BIGINT
        ),
    )


def _lower_func(sql: str) -> str:
    index = sql.index("(")
    return sql[:index].lower() + sql[index:]


def _quantile_sql(self: TimeplusSqlglotDialect.Generator, expression: exp.Quantile) -> str:
    quantile = expression.args["quantile"]
    args = f"({self.sql(expression, 'this')})"

    if isinstance(quantile, exp.Array):
        func = self.func("quantiles", *quantile)
    else:
        func = self.func("quantile", quantile)

    return func + args


def _build_count_if(args: t.List) -> exp.CountIf | exp.CombinedAggFunc:
    if len(args) == 1:
        return exp.CountIf(this=seq_get(args, 0))

    return exp.CombinedAggFunc(this="count_if", expressions=args, parts=("count", "If"))


def _build_str_to_date(args: t.List) -> exp.Cast | exp.Anonymous:
    if len(args) == 3:
        return exp.Anonymous(this="STR_TO_DATE", expressions=args)

    strtodate = exp.StrToDate.from_arg_list(args)
    return exp.cast(strtodate, exp.DataType.build(exp.DataType.Type.DATETIME))


def _datetime_delta_sql(name: str) -> t.Callable[[Generator, DatetimeDelta], str]:
    def _delta_sql(self: Generator, expression: DatetimeDelta) -> str:
        if not expression.unit:
            return rename_func(name)(self, expression)

        return self.func(
            name,
            unit_to_var(expression),
            expression.expression,
            expression.this,
        )

    return _delta_sql


def _timestrtotime_sql(self: TimeplusSqlglotDialect.Generator, expression: exp.TimeStrToTime):
    ts = expression.this

    tz = expression.args.get("zone")
    if tz and isinstance(ts, exp.Literal):
        # Timeplus will not accept timestamps that include a UTC offset, so we must remove them.
        # The first step to removing is parsing the string with `datetime.datetime.fromisoformat`.
        #
        # In python <3.11, `fromisoformat()` can only parse timestamps of millisecond (3 digit)
        # or microsecond (6 digit) precision. It will error if passed any other number of fractional
        # digits, so we extract the fractional seconds and pad to 6 digits before parsing.
        ts_string = ts.name.strip()

        # separate [date and time] from [fractional seconds and UTC offset]
        ts_parts = ts_string.split(".")
        if len(ts_parts) == 2:
            # separate fractional seconds and UTC offset
            offset_sep = "+" if "+" in ts_parts[1] else "-"
            ts_frac_parts = ts_parts[1].split(offset_sep)
            num_frac_parts = len(ts_frac_parts)

            # pad to 6 digits if fractional seconds present
            ts_frac_parts[0] = ts_frac_parts[0].ljust(6, "0")
            ts_string = "".join(
                [
                    ts_parts[0],  # date and time
                    ".",
                    ts_frac_parts[0],  # fractional seconds
                    offset_sep if num_frac_parts > 1 else "",
                    ts_frac_parts[1] if num_frac_parts > 1 else "",  # utc offset (if present)
                ]
            )

        # return literal with no timezone, eg turn '2020-01-01 12:13:14-08:00' into '2020-01-01 12:13:14'
        # this is because Timeplus encodes the timezone as a data type parameter and throws an error if
        # it's part of the timestamp string
        ts_without_tz = (
            datetime.datetime.fromisoformat(ts_string).replace(tzinfo=None).isoformat(sep=" ")
        )
        ts = exp.Literal.string(ts_without_tz)

    # Non-nullable DateTime64 with microsecond precision
    expressions = [exp.DataTypeParam(this=tz)] if tz else []
    datatype = exp.DataType.build(
        exp.DataType.Type.DATETIME64,
        expressions=[exp.DataTypeParam(this=exp.Literal.number(6)), *expressions],
        nullable=False,
    )

    return self.sql(exp.cast(ts, datatype, dialect=self.dialect))


def _map_sql(self: TimeplusSqlglotDialect.Generator, expression: exp.Map | exp.VarMap) -> str:
    if not (expression.parent and expression.parent.arg_key == "settings"):
        return _lower_func(var_map_sql(self, expression))

    keys = expression.args.get("keys")
    values = expression.args.get("values")

    if not isinstance(keys, exp.Array) or not isinstance(values, exp.Array):
        self.unsupported("Cannot convert array columns into map.")
        return ""

    args = []
    for key, value in zip(keys.expressions, values.expressions):
        args.append(f"{self.sql(key)}: {self.sql(value)}")

    csv_args = ", ".join(args)

    return f"{{{csv_args}}}"


class TimeplusSqlglotDialect(Dialect):
    NORMALIZE_FUNCTIONS: bool | str = False
    NULL_ORDERING = "nulls_are_last"
    SUPPORTS_USER_DEFINED_TYPES = False
    SAFE_DIVISION = True
    LOG_BASE_FIRST: t.Optional[bool] = None
    FORCE_EARLY_ALIAS_REF_EXPANSION = True
    PRESERVE_ORIGINAL_NAMES = True
    NUMBERS_CAN_BE_UNDERSCORE_SEPARATED = True
    IDENTIFIERS_CAN_START_WITH_DIGIT = True
    HEX_STRING_IS_INTEGER_TYPE = True

    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_SENSITIVE

    UNESCAPED_SEQUENCES = {
        "\\0": "\0",
    }

    CREATABLE_KIND_MAPPING = {"DATABASE": "SCHEMA"}

    SET_OP_DISTINCT_BY_DEFAULT: t.Dict[t.Type[exp.Expression], t.Optional[bool]] = {
        exp.Except: False,
        exp.Intersect: False,
        exp.Union: None,
    }

    class Tokenizer(tokens.Tokenizer):
        COMMENTS = ["--", "#", "#!", ("/*", "*/")]
        IDENTIFIERS = ['"', "`"]
        IDENTIFIER_ESCAPES = ["\\"]
        STRING_ESCAPES = ["'", "\\"]
        BIT_STRINGS = [("0b", "")]
        HEX_STRINGS = [("0x", ""), ("0X", "")]
        HEREDOC_STRINGS = ["$"]

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "ATTACH": TokenType.COMMAND,
            "DATE32": TokenType.DATE32,
            "DATETIME64": TokenType.DATETIME64,
            "DICTIONARY": TokenType.DICTIONARY,
            "ENUM8": TokenType.ENUM8,
            "ENUM16": TokenType.ENUM16,
            "FINAL": TokenType.FINAL,
            "FIXED_STRING": TokenType.FIXEDSTRING,
            "FLOAT32": TokenType.FLOAT,
            "FLOAT64": TokenType.DOUBLE,
            "GLOBAL": TokenType.GLOBAL,
            "LOW_CARDINALITY": TokenType.LOWCARDINALITY,
            "MAP": TokenType.MAP,
            "NESTED": TokenType.NESTED,
            "SAMPLE": TokenType.TABLE_SAMPLE,
            "TUPLE": TokenType.STRUCT,
            "UINT16": TokenType.USMALLINT,
            "UINT32": TokenType.UINT,
            "UINT64": TokenType.UBIGINT,
            "UINT8": TokenType.UTINYINT,
            "IPV4": TokenType.IPV4,
            "IPV6": TokenType.IPV6,
            # "POINT": TokenType.POINT,
            # "RING": TokenType.RING,
            # "LINESTRING": TokenType.LINESTRING,
            # "MULTILINESTRING": TokenType.MULTILINESTRING,
            # "POLYGON": TokenType.POLYGON,
            # "MULTIPOLYGON": TokenType.MULTIPOLYGON,
            "AGGREGATEFUNCTION": TokenType.AGGREGATEFUNCTION,
            "SIMPLEAGGREGATEFUNCTION": TokenType.SIMPLEAGGREGATEFUNCTION,
            "SYSTEM": TokenType.COMMAND,
            "PREWHERE": TokenType.PREWHERE,
        }
        KEYWORDS.pop("/*+")

        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
            "$": TokenType.HEREDOC_STRING,
        }

    classes = {}

    class Parser(parser.Parser):
        # Tested in Timeplus's playground, it seems that the following two queries do the same thing
        # * select x from t1 union all select x from t2 limit 1;
        # * select x from t1 union all (select x from t2 limit 1);
        MODIFIERS_ATTACHED_TO_SET_OP = False
        INTERVAL_SPANS = False
        OPTIONAL_ALIAS_TOKEN_CTE = False

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "ANY": exp.AnyValue.from_arg_list,
            "ARRAYSUM": exp.ArraySum.from_arg_list,
            "COUNTIF": _build_count_if,
            "DATE_ADD": build_date_delta(exp.DateAdd, default_unit=None),
            "DATEADD": build_date_delta(exp.DateAdd, default_unit=None),
            "DATE_DIFF": build_date_delta(exp.DateDiff, default_unit=None),
            "DATEDIFF": build_date_delta(exp.DateDiff, default_unit=None),
            "DATE_FORMAT": _build_date_format,
            "DATE_SUB": build_date_delta(exp.DateSub, default_unit=None),
            "DATESUB": build_date_delta(exp.DateSub, default_unit=None),
            "FORMATDATETIME": _build_date_format,
            "JSONEXTRACTSTRING": build_json_extract_path(
                exp.JSONExtractScalar, zero_based_indexing=False
            ),
            "LENGTH": lambda args: exp.Length(this=seq_get(args, 0), binary=True),
            "MAP": parser.build_var_map,
            "MATCH": exp.RegexpLike.from_arg_list,
            "RANDCANONICAL": exp.Rand.from_arg_list,
            "STR_TO_DATE": _build_str_to_date,
            "TUPLE": exp.Struct.from_arg_list,
            "TIMESTAMP_SUB": build_date_delta(exp.TimestampSub, default_unit=None),
            "TIMESTAMPSUB": build_date_delta(exp.TimestampSub, default_unit=None),
            "TIMESTAMP_ADD": build_date_delta(exp.TimestampAdd, default_unit=None),
            "TIMESTAMPADD": build_date_delta(exp.TimestampAdd, default_unit=None),
            "UNIQ": exp.ApproxDistinct.from_arg_list,
            "XOR": lambda args: exp.Xor(expressions=args),
            "MD5": exp.MD5Digest.from_arg_list,
            "SHA256": lambda args: exp.SHA2(this=seq_get(args, 0), length=exp.Literal.number(256)),
            "SHA512": lambda args: exp.SHA2(this=seq_get(args, 0), length=exp.Literal.number(512)),
            "EDITDISTANCE": exp.Levenshtein.from_arg_list,
            "LEVENSHTEINDISTANCE": exp.Levenshtein.from_arg_list,
        }
        FUNCTIONS.pop("TRANSFORM")

        AGG_FUNCTIONS = {
            "count",
            "min",
            "max",
            "sum",
            "avg",
            "any",
            "corr",
            "entropy",
            "median",
            "sumKahan",
            "bounding_ratio",
            "first_value",
            "last_value",
            "arg_min",
            "arg_max",
            "avg_weighted",
            "top_k",
            "top_k_weighted",
            "delta_sum",
            "delta_sum_timestamp",
            "group_array",
            "group_array_last",
            "group_uniq_array",
            "group_array_insert_at",
            "group_array_moving_avg",
            "group_array_moving_sum",
            "group_array_sample",
            "group_bit_and",
            "group_bit_or",
            "group_bit_xor",
            "group_bitmap",
            "group_bitmap_and",
            "group_bitmap_or",
            "group_bitmap_xor",
            "sum_with_overflow",
            "sum_map",
            "min_map",
            "max_map",
            "skew_samp",
            "skew_pop",
            "kurt_samp",
            "kurt_pop",
            "uniq",
            "uniq_exact",
            "uniq_combined",
            "uniq_combined64",
            "uniq_theta",
            "quantile",
            "quantiles",
            "quantile_exact",
            "quantiles_exact",
            "quantile_exact_low",
            "quantiles_exact_low",
            "quantile_exact_high",
            "quantiles_exact_high",
            "quantile_exact_weighted",
            "quantiles_exact_weighted",
            "quantile_timing",
            "quantiles_timing",
            "quantile_timing_weighted",
            "quantiles_timing_weighted",
            "quantile_deterministic",
            "quantiles_deterministic",
            "quantile_tdigest",
            "quantiles_tdigest",
            "quantile_tdigest_weighted",
            "quantiles_tdigest_weighted",
            "simple_linear_regression",
            "stochastic_linear_regression",
            "stochastic_logistic_regression",
            "categorical_information_value",
            "contingency",
            "max_intersections",
            "max_intersections_position",
            "quantile_interpolated_weighted",
            "quantiles_interpolated_weighted",
            "spark_bar",
            "sum_count",
            "largest_triangle_three_buckets",
            "histogram",
            "sequence_match",
            "sequence_count",
            "retention",
        }

        AGG_FUNCTIONS_SUFFIXES = [
            "if",
            "array",
            "array_if",
            "map",
            "simple_state",
            "state",
            "merge",
            "merge_state",
            "for_each",
            "distinct",
            "or_default",
            "or_null",
            "resample",
            "arg_min",
            "arg_max",
        ]

        FUNC_TOKENS = {
            *parser.Parser.FUNC_TOKENS,
            TokenType.SET,
        }

        RESERVED_TOKENS = parser.Parser.RESERVED_TOKENS - {TokenType.SELECT}

        ID_VAR_TOKENS = {
            *parser.Parser.ID_VAR_TOKENS,
            TokenType.LIKE,
        }

        # pylint: disable=unnecessary-direct-lambda-call
        AGG_FUNC_MAPPING = (
            lambda functions, suffixes : {
                f"{f}{sfx}": (f, sfx) for sfx in (suffixes + [""]) for f in functions
            }
        )(AGG_FUNCTIONS, AGG_FUNCTIONS_SUFFIXES)

        FUNCTIONS_WITH_ALIASED_ARGS = {*parser.Parser.FUNCTIONS_WITH_ALIASED_ARGS, "TUPLE"}

        # pylint: disable=protected-access
        FUNCTION_PARSERS = {
            **parser.Parser.FUNCTION_PARSERS,
            "ARRAYJOIN": lambda self: self.expression(exp.Explode, this=self._parse_expression()),
            "QUANTILE": lambda self: self._parse_quantile(),
            "MEDIAN": lambda self: self._parse_quantile(),
            "COLUMNS": lambda self: self._parse_columns(),
        }

        FUNCTION_PARSERS.pop("MATCH")

        PROPERTY_PARSERS = parser.Parser.PROPERTY_PARSERS.copy()
        PROPERTY_PARSERS.pop("DYNAMIC")

        NO_PAREN_FUNCTION_PARSERS = parser.Parser.NO_PAREN_FUNCTION_PARSERS.copy()
        NO_PAREN_FUNCTION_PARSERS.pop("ANY")

        NO_PAREN_FUNCTIONS = parser.Parser.NO_PAREN_FUNCTIONS.copy()
        NO_PAREN_FUNCTIONS.pop(TokenType.CURRENT_TIMESTAMP)

        RANGE_PARSERS = {
            **parser.Parser.RANGE_PARSERS,
            TokenType.GLOBAL: lambda self, this: self._match(TokenType.IN)
            and self._parse_in(this, is_global=True),
        }

        # The PLACEHOLDER entry is popped because 1) it doesn't affect Timeplus (it corresponds to
        # the postgres-specific JSONBContains parser) and 2) it makes parsing the ternary op simpler.
        COLUMN_OPERATORS = parser.Parser.COLUMN_OPERATORS.copy()
        COLUMN_OPERATORS.pop(TokenType.PLACEHOLDER)

        JOIN_KINDS = {
            *parser.Parser.JOIN_KINDS,
            TokenType.ANY,
            TokenType.ASOF,
            TokenType.ARRAY,
        }

        TABLE_ALIAS_TOKENS = parser.Parser.TABLE_ALIAS_TOKENS - {
            TokenType.ANY,
            TokenType.ARRAY,
            TokenType.FINAL,
            TokenType.FORMAT,
            TokenType.SETTINGS,
        }

        ALIAS_TOKENS = parser.Parser.ALIAS_TOKENS - {
            TokenType.FORMAT,
        }

        LOG_DEFAULTS_TO_LN = True

        QUERY_MODIFIER_PARSERS = {
            **parser.Parser.QUERY_MODIFIER_PARSERS,
            TokenType.SETTINGS: lambda self: (
                "settings",
                self._advance() or self._parse_csv(self._parse_assignment),
            ),
            TokenType.FORMAT: lambda self: ("format", self._advance() or self._parse_id_var()),
        }

        CONSTRAINT_PARSERS = {
            **parser.Parser.CONSTRAINT_PARSERS,
            "INDEX": lambda self: self._parse_index_constraint(),
            "CODEC": lambda self: self._parse_compress(),
        }

        ALTER_PARSERS = {
            **parser.Parser.ALTER_PARSERS,
            "REPLACE": lambda self: self._parse_alter_table_replace(),
        }

        SCHEMA_UNNAMED_CONSTRAINTS = {
            *parser.Parser.SCHEMA_UNNAMED_CONSTRAINTS,
            "INDEX",
        }

        PLACEHOLDER_PARSERS = {
            **parser.Parser.PLACEHOLDER_PARSERS,
            TokenType.L_BRACE: lambda self: self._parse_query_parameter(),
        }

        def _parse_user_defined_function_expression(self) -> t.Optional[exp.Expression]:
            return self._parse_lambda()

        def _parse_types(
            self, check_func: bool = False, schema: bool = False, allow_identifiers: bool = True
        ) -> t.Optional[exp.Expression]:
            dtype = super()._parse_types(
                check_func=check_func, schema=schema, allow_identifiers=allow_identifiers
            )
            if isinstance(dtype, exp.DataType) and dtype.args.get("nullable") is not True:
                # Mark every type as non-nullable which is Timeplus's default, unless it's
                # already marked as nullable. This marker helps us transpile types from other
                # dialects to Timeplus, so that we can e.g. produce `CAST(x AS nullable(String))`
                # from `CAST(x AS TEXT)`. If there is a `NULL` value in `x`, the former would
                # fail in Timeplus without the `nullable` type constructor.
                dtype.set("nullable", False)

            return dtype

        def _parse_extract(self) -> exp.Extract | exp.Anonymous:
            index = self._index
            this = self._parse_bitwise()
            if self._match(TokenType.FROM):
                self._retreat(index)
                return super()._parse_extract()

            # We return Anonymous here because extract and regexpExtract have different semantics,
            # so parsing extract(foo, bar) into RegexpExtract can potentially break queries. E.g.,
            # `extract('foobar', 'b')` works, but Timeplus crashes for `regexp_extract('foobar', 'b')`.
            #
            # TODO: can we somehow convert the former into an equivalent `regexpExtract` call?
            self._match(TokenType.COMMA)
            return self.expression(
                exp.Anonymous, this="extract", expressions=[this, self._parse_bitwise()]
            )

        def _parse_assignment(self) -> t.Optional[exp.Expression]:
            this = super()._parse_assignment()

            if self._match(TokenType.PLACEHOLDER):
                return self.expression(
                    exp.If,
                    this=this,
                    true=self._parse_assignment(),
                    false=self._match(TokenType.COLON) and self._parse_assignment(),
                )

            return this

        def _parse_query_parameter(self) -> t.Optional[exp.Expression]:
            """
            Parse a placeholder expression like SELECT {abc: uint32} or FROM {table: Identifier}
            """
            index = self._index

            this = self._parse_id_var()
            self._match(TokenType.COLON)
            kind = self._parse_types(check_func=False, allow_identifiers=False) or (
                self._match_text_seq("IDENTIFIER") and "Identifier"
            )

            if not kind:
                self._retreat(index)
                return None

            if not self._match(TokenType.R_BRACE):
                self.raise_error("Expecting }")

            return self.expression(exp.Placeholder, this=this, kind=kind)

        def _parse_bracket(
            self, this: t.Optional[exp.Expression] = None
        ) -> t.Optional[exp.Expression]:
            l_brace = self._match(TokenType.L_BRACE, advance=False)
            bracket = super()._parse_bracket(this)

            if l_brace and isinstance(bracket, exp.Struct):
                varmap = exp.VarMap(keys=exp.Array(), values=exp.Array())
                for expression in bracket.expressions:
                    if not isinstance(expression, exp.PropertyEQ):
                        break

                    varmap.args["keys"].append("expressions", exp.Literal.string(expression.name))
                    varmap.args["values"].append("expressions", expression.expression)

                return varmap

            return bracket

        # pylint: disable=arguments-renamed
        def _parse_in(self, this: t.Optional[exp.Expression], is_global: bool = False) -> exp.In:
            this = super()._parse_in(this)
            this.set("is_global", is_global)
            return this

        def _parse_table(
            self,
            schema: bool = False,
            joins: bool = False,
            alias_tokens: t.Optional[t.Collection[TokenType]] = None,
            parse_bracket: bool = False,
            is_db_reference: bool = False,
            parse_partition: bool = False, # pylint: disable=unused-argument
        ) -> t.Optional[exp.Expression]:
            this = super()._parse_table(
                schema=schema,
                joins=joins,
                alias_tokens=alias_tokens,
                parse_bracket=parse_bracket,
                is_db_reference=is_db_reference,
            )

            if isinstance(this, exp.Table):
                inner = this.this
                alias = this.args.get("alias")

                if isinstance(inner, exp.GenerateSeries) and alias and not alias.columns:
                    alias.set("columns", [exp.to_identifier("generate_series")])

            if self._match(TokenType.FINAL):
                this = self.expression(exp.Final, this=this)

            return this

        # pylint: disable=unused-argument
        def _parse_position(self, haystack_first: bool = False) -> exp.StrPosition:
            return super()._parse_position(haystack_first=True)

        def _parse_cte(self) -> t.Optional[exp.CTE]:
            # WITH <identifier> AS <subquery expression>
            cte: t.Optional[exp.CTE] = self._try_parse(super()._parse_cte)

            if not cte:
                # WITH <expression> AS <identifier>
                cte = self.expression(
                    exp.CTE,
                    this=self._parse_assignment(),
                    alias=self._parse_table_alias(),
                    scalar=True,
                )

            return cte

        def _parse_join_parts(
            self,
        ) -> t.Tuple[t.Optional[Token], t.Optional[Token], t.Optional[Token]]:
            is_global = self._match(TokenType.GLOBAL) and self._prev
            kind_pre = self._match_set(self.JOIN_KINDS, advance=False) and self._prev

            if kind_pre:
                kind = self._match_set(self.JOIN_KINDS) and self._prev
                side = self._match_set(self.JOIN_SIDES) and self._prev
                return is_global, side, kind

            return (
                is_global,
                self._match_set(self.JOIN_SIDES) and self._prev,
                self._match_set(self.JOIN_KINDS) and self._prev,
            )

        def _parse_join(
            self, skip_join_token: bool = False, parse_bracket: bool = False # pylint: disable=unused-argument
        ) -> t.Optional[exp.Join]:
            join = super()._parse_join(skip_join_token=skip_join_token, parse_bracket=True)
            if join:
                join.set("global", join.args.pop("method", None))

                # tbl ARRAY JOIN arr <-- this should be a `Column` reference, not a `Table`
                if join.kind == "array":
                    for table in join.find_all(exp.Table):
                        table.replace(table.to_column())

            return join

        def _parse_function(
            self,
            functions: t.Optional[t.Dict[str, t.Callable]] = None,
            anonymous: bool = False,
            optional_parens: bool = True,
            any_token: bool = False,
        ) -> t.Optional[exp.Expression]:
            expr = super()._parse_function(
                functions=functions,
                anonymous=anonymous,
                optional_parens=optional_parens,
                any_token=any_token,
            )

            func = expr.this if isinstance(expr, exp.Window) else expr

            # Aggregate functions can be split in 2 parts: <func_name><suffix>
            parts = (
                self.AGG_FUNC_MAPPING.get(func.this) if isinstance(func, exp.Anonymous) else None
            )

            if parts:
                anon_func: exp.Anonymous = t.cast(exp.Anonymous, func)
                params = self._parse_func_params(anon_func)

                kwargs = {
                    "this": anon_func.this,
                    "expressions": anon_func.expressions,
                }
                if parts[1]:
                    kwargs["parts"] = parts
                    exp_class: t.Type[exp.Expression] = (
                        exp.CombinedParameterizedAgg if params else exp.CombinedAggFunc
                    )
                else:
                    exp_class = exp.ParameterizedAgg if params else exp.AnonymousAggFunc

                kwargs["exp_class"] = exp_class
                if params:
                    kwargs["params"] = params

                func = self.expression(**kwargs)

                if isinstance(expr, exp.Window):
                    # The window's func was parsed as Anonymous in base parser, fix its
                    # type to be Timeplus style CombinedAnonymousAggFunc / AnonymousAggFunc
                    expr.set("this", func)
                elif params:
                    # Params have blocked super()._parse_function() from parsing the following window
                    # (if that exists) as they're standing between the function call and the window spec
                    expr = self._parse_window(func)
                else:
                    expr = func

            return expr

        def _parse_func_params(
            self, this: t.Optional[exp.Func] = None
        ) -> t.Optional[t.List[exp.Expression]]:
            if self._match_pair(TokenType.R_PAREN, TokenType.L_PAREN):
                return self._parse_csv(self._parse_lambda)

            if self._match(TokenType.L_PAREN):
                params = self._parse_csv(self._parse_lambda)
                self._match_r_paren(this)
                return params

            return None

        def _parse_quantile(self) -> exp.Quantile:
            this = self._parse_lambda()
            params = self._parse_func_params()
            if params:
                return self.expression(exp.Quantile, this=params[0], quantile=this)
            return self.expression(exp.Quantile, this=this, quantile=exp.Literal.number(0.5))

        # pylint: disable=unused-argument
        def _parse_wrapped_id_vars(self, optional: bool = False) -> t.List[exp.Expression]:
            return super()._parse_wrapped_id_vars(optional=True)

        def _parse_primary_key(
            self, wrapped_optional: bool = False, in_props: bool = False
        ) -> exp.PrimaryKeyColumnConstraint | exp.PrimaryKey:
            return super()._parse_primary_key(
                wrapped_optional=wrapped_optional or in_props, in_props=in_props
            )

        def _parse_on_property(self) -> t.Optional[exp.Expression]:
            index = self._index
            if self._match_text_seq("CLUSTER"):
                this = self._parse_id_var()
                if this:
                    return self.expression(exp.OnCluster, this=this)

                self._retreat(index)
            return None

        def _parse_index_constraint(self) -> exp.IndexColumnConstraint:
            # INDEX name1 expr TYPE type1(args) GRANULARITY value
            this = self._parse_id_var()
            expression = self._parse_assignment()

            index_type = self._match_text_seq("TYPE") and (
                self._parse_function() or self._parse_var()
            )

            granularity = self._match_text_seq("GRANULARITY") and self._parse_term()

            return self.expression(
                exp.IndexColumnConstraint,
                this=this,
                expression=expression,
                index_type=index_type,
                granularity=granularity,
            )

        def _parse_partition(self) -> t.Optional[exp.Partition]:
            if not self._match(TokenType.PARTITION):
                return None

            if self._match_text_seq("ID"):
                # Corresponds to the PARTITION ID <string_value> syntax
                expressions: t.List[exp.Expression] = [
                    self.expression(exp.PartitionId, this=self._parse_string())
                ]
            else:
                expressions = self._parse_expressions()

            return self.expression(exp.Partition, expressions=expressions)

        def _parse_alter_table_replace(self) -> t.Optional[exp.Expression]:
            partition = self._parse_partition()

            if not partition or not self._match(TokenType.FROM):
                return None

            return self.expression(
                exp.ReplacePartition, expression=partition, source=self._parse_table_parts()
            )

        def _parse_projection_def(self) -> t.Optional[exp.ProjectionDef]:
            if not self._match_text_seq("PROJECTION"):
                return None

            return self.expression(
                exp.ProjectionDef,
                this=self._parse_id_var(),
                expression=self._parse_wrapped(self._parse_statement),
            )

        def _parse_constraint(self) -> t.Optional[exp.Expression]:
            return super()._parse_constraint() or self._parse_projection_def()

        def _parse_alias(
            self, this: t.Optional[exp.Expression], explicit: bool = False
        ) -> t.Optional[exp.Expression]:
            # In Timeplus "SELECT <expr> APPLY(...)" is a query modifier,
            # so "APPLY" shouldn't be parsed as <expr>'s alias. However, "SELECT <expr> apply" is a valid alias
            if self._match_pair(TokenType.APPLY, TokenType.L_PAREN, advance=False):
                return this

            return super()._parse_alias(this=this, explicit=explicit)

        def _parse_expression(self) -> t.Optional[exp.Expression]:
            this = super()._parse_expression()

            # Timeplus allows "SELECT <expr> [APPLY(func)] [...]]" modifier
            while self._match_pair(TokenType.APPLY, TokenType.L_PAREN):
                this = exp.Apply(this=this, expression=self._parse_var(any_token=True))
                self._match(TokenType.R_PAREN)

            return this

        def _parse_columns(self) -> exp.Expression:
            this: exp.Expression = self.expression(exp.Columns, this=self._parse_lambda())

            while self._next and self._match_text_seq(")", "APPLY", "("):
                self._match(TokenType.R_PAREN)
                this = exp.Apply(this=this, expression=self._parse_var(any_token=True))
            return this

    # pylint: disable=too-many-public-methods
    class Generator(generator.Generator):
        QUERY_HINTS = False
        STRUCT_DELIMITER = ("(", ")")
        NVL2_SUPPORTED = False
        TABLESAMPLE_REQUIRES_PARENS = False
        TABLESAMPLE_SIZE_IS_ROWS = False
        TABLESAMPLE_KEYWORDS = "SAMPLE"
        LAST_DAY_SUPPORTS_DATE_PART = False
        CAN_IMPLEMENT_ARRAY_ANY = True
        SUPPORTS_TO_NUMBER = False
        JOIN_HINTS = False
        TABLE_HINTS = False
        GROUPINGS_SEP = ""
        SET_OP_MODIFIERS = False
        VALUES_AS_TABLE = False
        ARRAY_SIZE_NAME = "LENGTH"

        STRING_TYPE_MAPPING = {
            exp.DataType.Type.CHAR: "string",
            exp.DataType.Type.LONGBLOB: "string",
            exp.DataType.Type.LONGTEXT: "string",
            exp.DataType.Type.MEDIUMBLOB: "string",
            exp.DataType.Type.MEDIUMTEXT: "string",
            exp.DataType.Type.TINYBLOB: "string",
            exp.DataType.Type.TINYTEXT: "string",
            exp.DataType.Type.TEXT: "string",
            exp.DataType.Type.VARBINARY: "string",
            exp.DataType.Type.VARCHAR: "string",
        }

        SUPPORTED_JSON_PATH_PARTS = {
            exp.JSONPathKey,
            exp.JSONPathRoot,
            exp.JSONPathSubscript,
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            **STRING_TYPE_MAPPING,
            exp.DataType.Type.ARRAY: "array",
            exp.DataType.Type.BOOLEAN: "bool",
            exp.DataType.Type.BIGINT: "int64",
            exp.DataType.Type.DATE32: "Date32",
            exp.DataType.Type.DATETIME: "DateTime",
            exp.DataType.Type.DATETIME64: "DateTime64",
            exp.DataType.Type.DECIMAL: "Decimal",
            exp.DataType.Type.DECIMAL32: "Decimal32",
            exp.DataType.Type.DECIMAL64: "Decimal64",
            exp.DataType.Type.DECIMAL128: "Decimal128",
            # superset use the sqlglot with version 25.24.0 which is not support DECIMAL256.
            # exp.DataType.Type.DECIMAL256: "Decimal256",
            exp.DataType.Type.TIMESTAMP: "DateTime",
            exp.DataType.Type.TIMESTAMPTZ: "DateTime",
            exp.DataType.Type.DOUBLE: "float64",
            exp.DataType.Type.ENUM: "enum",
            exp.DataType.Type.ENUM8: "enum8",
            exp.DataType.Type.ENUM16: "enum16",
            exp.DataType.Type.FIXEDSTRING: "fixed_string",
            exp.DataType.Type.FLOAT: "float32",
            exp.DataType.Type.INT: "int32",
            exp.DataType.Type.MEDIUMINT: "int32",
            exp.DataType.Type.INT128: "int128",
            exp.DataType.Type.INT256: "int256",
            exp.DataType.Type.LOWCARDINALITY: "low_cardinality",
            exp.DataType.Type.MAP: "map",
            exp.DataType.Type.NESTED: "nested",
            exp.DataType.Type.SMALLINT: "int16",
            exp.DataType.Type.STRUCT: "tuple",
            exp.DataType.Type.TINYINT: "int8",
            exp.DataType.Type.UBIGINT: "uint64",
            exp.DataType.Type.UINT: "uint32",
            exp.DataType.Type.UINT128: "uint128",
            exp.DataType.Type.UINT256: "uint256",
            exp.DataType.Type.USMALLINT: "uint16",
            exp.DataType.Type.UTINYINT: "uint8",
            exp.DataType.Type.IPV4: "ipv4",
            exp.DataType.Type.IPV6: "ipv6",
            exp.DataType.Type.AGGREGATEFUNCTION: "aggregate_function",
            exp.DataType.Type.SIMPLEAGGREGATEFUNCTION: "simple_aggregate_function",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.AnyValue: rename_func("any"),
            exp.ApproxDistinct: rename_func("uniq"),
            exp.ArrayConcat: rename_func("array_concat"),
            exp.ArrayFilter: lambda self, e: self.func("array_filter", e.expression, e.this),
            exp.ArraySum: rename_func("array_sum"),
            exp.ArgMax: arg_max_or_min_no_count("arg_max"),
            exp.ArgMin: arg_max_or_min_no_count("arg_min"),
            exp.Array: inline_array_sql,
            exp.CastToStrType: rename_func("CAST"),
            exp.CountIf: rename_func("count_if"),
            exp.CompressColumnConstraint: lambda self,
            e: f"CODEC({self.expressions(e, key='this', flat=True)})",
            exp.ComputedColumnConstraint: lambda self,
            e: f"{'MATERIALIZED' if e.args.get('persisted') else 'ALIAS'} {self.sql(e, 'this')}",
            exp.CurrentDate: lambda self, e: self.func("CURRENT_DATE"),
            exp.DateAdd: _datetime_delta_sql("date_add"),
            exp.DateDiff: _datetime_delta_sql("date_diff"),
            exp.DateStrToDate: rename_func("to_date"),
            exp.DateSub: _datetime_delta_sql("date_sub"),
            exp.Explode: rename_func("array_join"),
            exp.Final: lambda self, e: f"{self.sql(e, 'this')} FINAL",
            exp.IsNan: rename_func("is_nan"),
            exp.IsInf: rename_func("is_infinite"),
            exp.JSONExtract: json_extract_segments("json_extract_string", quoted_index=False),
            exp.JSONExtractScalar: json_extract_segments("json_extract_string", quoted_index=False),
            exp.JSONPathKey: json_path_key_only_name,
            exp.JSONPathRoot: lambda *_: "",
            exp.Map: _map_sql,
            # superset use the sqlglot with version 25.24.0 which is not support Median.
            # exp.Median: rename_func("median"),
            exp.PartitionedByProperty: lambda self, e: f"PARTITION BY {self.sql(e, 'this')}",
            exp.Pivot: no_pivot_sql,
            exp.Quantile: _quantile_sql,
            exp.RegexpLike: lambda self, e: self.func("match", e.this, e.expression),
            exp.Rand: rename_func("rand"),
            exp.StartsWith: rename_func("start_with"),
            exp.TimeToStr: lambda self, e: self.func(
                "format_datetime", e.this, self.format_time(e), e.args.get("zone")
            ),
            exp.TimeStrToTime: _timestrtotime_sql,
            exp.TimestampAdd: _datetime_delta_sql("TIMESTAMP_ADD"),
            exp.TimestampSub: _datetime_delta_sql("TIMESTAMP_SUB"),
            exp.VarMap: _map_sql,
            exp.Xor: lambda self, e: self.func("xor", e.this, e.expression, *e.expressions),
            exp.MD5Digest: rename_func("md5"),
            exp.MD5: lambda self, e: self.func("LOWER", self.func("HEX", self.func("md5", e.this))),
            exp.SHA: rename_func("sha1"),
            exp.SHA2: sha256_sql,
            exp.UnixToTime: _unix_to_time_sql,
            exp.TimestampTrunc: timestamptrunc_sql(zone=True),
            exp.Trim: trim_sql,
            # exp.Variance: rename_func("varSamp"),
            exp.SchemaCommentProperty: lambda self, e: self.naked_property(e),
            # exp.Stddev: rename_func("stddevSamp"),
            # exp.Chr: rename_func("CHAR"),
            exp.Lag: lambda self, e: self.func(
                "lagInFrame", e.this, e.args.get("offset"), e.args.get("default")
            ),
            exp.Lead: lambda self, e: self.func(
                "leadInFrame", e.this, e.args.get("offset"), e.args.get("default")
            ),
            exp.Levenshtein: unsupported_args("ins_cost", "del_cost", "sub_cost", "max_dist")(
                rename_func("editDistance")
            ),
        }

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
            exp.OnCluster: exp.Properties.Location.POST_NAME,
            exp.PartitionedByProperty: exp.Properties.Location.POST_SCHEMA,
            exp.ToTableProperty: exp.Properties.Location.POST_NAME,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
        }

        # There's no list in docs, but it can be found in Timeplus code
        ON_CLUSTER_TARGETS = {
            "SCHEMA",  # Transpiled CREATE SCHEMA may have OnCluster property set
            "DATABASE",
            "STREAM",
            "VIEW",
            "DICTIONARY",
            "INDEX",
            "FUNCTION",
            "NAMED COLLECTION",
        }

        NON_NULLABLE_TYPES = {
            exp.DataType.Type.ARRAY,
            exp.DataType.Type.MAP,
            exp.DataType.Type.STRUCT,
        }

        def strtodate_sql(self, expression: exp.StrToDate) -> str:
            strtodate_sql = self.function_fallback_sql(expression)

            if not isinstance(expression.parent, exp.Cast):
                # StrToDate returns DATEs in other dialects (eg. postgres), so
                # this branch aims to improve the transpilation to Timeplus
                return self.cast_sql(exp.cast(expression, "DATE"))

            return strtodate_sql

        def cast_sql(self, expression: exp.Cast, safe_prefix: t.Optional[str] = None) -> str:
            this = expression.this

            if isinstance(this, exp.StrToDate) and expression.to == exp.DataType.build("datetime"):
                return self.sql(this)

            return super().cast_sql(expression, safe_prefix=safe_prefix)

        def trycast_sql(self, expression: exp.TryCast) -> str:
            dtype = expression.to
            if not dtype.is_type(*self.NON_NULLABLE_TYPES, check_nullable=True):
                # Casting x into nullable(T) appears to behave similarly to TRY_CAST(x AS T)
                dtype.set("nullable", True)

            return super().cast_sql(expression)

        def _jsonpathsubscript_sql(self, expression: exp.JSONPathSubscript) -> str:
            this = self.json_path_part(expression.this)
            return str(int(this) + 1) if is_int(this) else this

        def likeproperty_sql(self, expression: exp.LikeProperty) -> str:
            return f"AS {self.sql(expression, 'this')}"

        def _any_to_has(
            self,
            expression: exp.EQ | exp.NEQ,
            default: t.Callable[[t.Any], str],
            prefix: str = "",
        ) -> str:
            if isinstance(expression.left, exp.Any):
                arr = expression.left
                this = expression.right
            elif isinstance(expression.right, exp.Any):
                arr = expression.right
                this = expression.left
            else:
                return default(expression)

            return prefix + self.func("has", arr.this.unnest(), this)

        def eq_sql(self, expression: exp.EQ) -> str:
            return self._any_to_has(expression, super().eq_sql)

        def neq_sql(self, expression: exp.NEQ) -> str:
            return self._any_to_has(expression, super().neq_sql, "NOT ")

        def regexpilike_sql(self, expression: exp.RegexpILike) -> str:
            # Manually add a flag to make the search case-insensitive
            regex = self.func("CONCAT", "'(?i)'", expression.expression)
            return self.func("match", expression.this, regex)

        def datatype_sql(self, expression: exp.DataType) -> str:
            # String is the standard Timeplus type, every other variant is just an alias.
            # Additionally, any supplied length parameter will be ignored.
            if expression.this in self.STRING_TYPE_MAPPING:
                dtype = "string"
            else:
                dtype = super().datatype_sql(expression)

            # This section changes the type to `nullable(...)` if the following conditions hold:
            # - It's marked as nullable - this ensures we won't wrap Timeplus types with `nullable`
            #   and change their semantics
            # - It's not the key type of a `Map`. This is because Timeplus enforces the following
            #   constraint: "Type of Map key must be a type, that can be represented by integer or
            #   String or FixedString (possibly LowCardinality) or UUID or IPv6"
            # - It's not a composite type, e.g. `nullable(array(...))` is not a valid type
            parent = expression.parent
            nullable = expression.args.get("nullable")
            if nullable is True or (
                nullable is None
                and not (
                    isinstance(parent, exp.DataType)
                    and parent.is_type(exp.DataType.Type.MAP, check_nullable=True)
                    and expression.index in (None, 0)
                )
                and not expression.is_type(*self.NON_NULLABLE_TYPES, check_nullable=True)
            ):
                dtype = f"nullable({dtype})"

            return dtype

        def cte_sql(self, expression: exp.CTE) -> str:
            if expression.args.get("scalar"):
                this = self.sql(expression, "this")
                alias = self.sql(expression, "alias")
                return f"{this} AS {alias}"

            return super().cte_sql(expression)

        def after_limit_modifiers(self, expression: exp.Expression) -> t.List[str]:
            return super().after_limit_modifiers(expression) + [
                (
                    self.seg("SETTINGS ") + self.expressions(expression, key="settings", flat=True)
                    if expression.args.get("settings")
                    else ""
                ),
                (
                    self.seg("FORMAT ") + self.sql(expression, "format")
                    if expression.args.get("format")
                    else ""
                ),
            ]

        def parameterizedagg_sql(self, expression: exp.ParameterizedAgg) -> str:
            params = self.expressions(expression, key="params", flat=True)
            return self.func(expression.name, *expression.expressions) + f"({params})"

        def anonymousaggfunc_sql(self, expression: exp.AnonymousAggFunc) -> str:
            return self.func(expression.name, *expression.expressions)

        def combinedaggfunc_sql(self, expression: exp.CombinedAggFunc) -> str:
            return self.anonymousaggfunc_sql(expression)

        def combinedparameterizedagg_sql(self, expression: exp.CombinedParameterizedAgg) -> str:
            return self.parameterizedagg_sql(expression)

        def placeholder_sql(self, expression: exp.Placeholder) -> str:
            return f"{{{expression.name}: {self.sql(expression, 'kind')}}}"

        def oncluster_sql(self, expression: exp.OnCluster) -> str:
            return f"ON CLUSTER {self.sql(expression, 'this')}"

        def createable_sql(self, expression: exp.Create, locations: t.DefaultDict) -> str:
            if expression.kind in self.ON_CLUSTER_TARGETS and locations.get(
                exp.Properties.Location.POST_NAME
            ):
                this_name = self.sql(
                    expression.this if isinstance(expression.this, exp.Schema) else expression,
                    "this",
                )
                this_properties = " ".join(
                    [self.sql(prop) for prop in locations[exp.Properties.Location.POST_NAME]]
                )
                this_schema = self.schema_columns_sql(expression.this)
                this_schema = f"{self.sep()}{this_schema}" if this_schema else ""

                return f"{this_name}{self.sep()}{this_properties}{this_schema}"

            return super().createable_sql(expression, locations)

        def create_sql(self, expression: exp.Create) -> str:
            # The comment property comes last in CTAS statements, i.e. after the query
            query = expression.expression
            if isinstance(query, exp.Query):
                comment_prop = expression.find(exp.SchemaCommentProperty)
                if comment_prop:
                    comment_prop.pop()
                    query.replace(exp.paren(query))
            else:
                comment_prop = None

            create_sql = super().create_sql(expression)

            comment_sql = self.sql(comment_prop)
            comment_sql = f" {comment_sql}" if comment_sql else ""

            return f"{create_sql}{comment_sql}"

        def prewhere_sql(self, expression: exp.PreWhere) -> str:
            this = self.indent(self.sql(expression, "this"))
            return f"{self.seg('PREWHERE')}{self.sep()}{this}"

        def indexcolumnconstraint_sql(self, expression: exp.IndexColumnConstraint) -> str:
            this = self.sql(expression, "this")
            this = f" {this}" if this else ""
            expr = self.sql(expression, "expression")
            expr = f" {expr}" if expr else ""
            index_type = self.sql(expression, "index_type")
            index_type = f" TYPE {index_type}" if index_type else ""
            granularity = self.sql(expression, "granularity")
            granularity = f" GRANULARITY {granularity}" if granularity else ""

            return f"INDEX{this}{expr}{index_type}{granularity}"

        def partition_sql(self, expression: exp.Partition) -> str:
            return f"PARTITION {self.expressions(expression, flat=True)}"

        def partitionid_sql(self, expression: exp.PartitionId) -> str:
            return f"ID {self.sql(expression.this)}"

        def replacepartition_sql(self, expression: exp.ReplacePartition) -> str:
            return (
                f"REPLACE {self.sql(expression.expression)} FROM {self.sql(expression, 'source')}"
            )

        def projectiondef_sql(self, expression: exp.ProjectionDef) -> str:
            return f"PROJECTION {self.sql(expression.this)} {self.wrap(expression.expression)}"

        def is_sql(self, expression: exp.Is) -> str:
            is_sql = super().is_sql(expression)

            if isinstance(expression.parent, exp.Not):
                # value IS NOT NULL -> NOT (value IS NULL)
                is_sql = self.wrap(is_sql)

            return is_sql

    tokenizer_class = Tokenizer
    parser_class = Parser
    generator_class = Generator
