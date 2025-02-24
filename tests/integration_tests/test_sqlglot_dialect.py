
from sqlglot import exp, parse_one
from timeplus_connect.tp_sqlglot.dialect import TimeplusSqlglotDialect


dialect = TimeplusSqlglotDialect()

def validate_identity(sql: str) -> exp.Expression:
    ast: exp.Expression = parse_one(sql, read=dialect)

    assert ast is not None
    assert ast.sql(dialect=dialect) == sql

    return ast

def test_simple_sql():
    validate_identity("CAST(1 AS bool)")
    validate_identity("SELECT to_string(CHAR(104.1, 101, 108.9, 108.9, 111, 32))")
    validate_identity("@macro").assert_is(exp.Parameter).this.assert_is(exp.Var)
    validate_identity("SELECT to_float(like)")
    validate_identity("SELECT like")
    validate_identity("SELECT EXTRACT(YEAR FROM to_datetime('2023-02-01'))")
    validate_identity("extract(haystack, pattern)")
    validate_identity("SELECT * FROM x LIMIT 1 UNION ALL SELECT * FROM y")
    validate_identity("SELECT CAST(x AS tuple(string, array(nullable(float64))))")
    validate_identity("count_if(x)")
    validate_identity("x = y")
    validate_identity("x <> y")
    validate_identity("SELECT * FROM (SELECT a FROM b SAMPLE 0.01)")
    validate_identity("SELECT * FROM (SELECT a FROM b SAMPLE 1 / 10 OFFSET 1 / 2)")
    validate_identity("SELECT sum(foo * bar) FROM bla SAMPLE 10000000")
    validate_identity("CAST(x AS nested(ID uint32, Serial uint32, EventTime DateTime))")
    validate_identity("CAST(x AS enum('hello' = 1, 'world' = 2))")
    validate_identity("CAST(x AS enum('hello', 'world'))")
    validate_identity("CAST(x AS enum('hello' = 1, 'world'))")
    validate_identity("CAST(x AS enum8('hello' = -123, 'world'))")
    validate_identity("CAST(x AS fixed_string(1))")
    validate_identity("CAST(x AS low_cardinality(fixed_string))")
    validate_identity("SELECT is_nan(1.0)")
    validate_identity("SELECT start_with('Spider-Man', 'Spi')")
    validate_identity("SELECT xor(TRUE, FALSE)")
    validate_identity("CAST(['hello'], 'array(enum8(''hello'' = 1))')")
    validate_identity("SELECT x, COUNT() FROM y GROUP BY x WITH TOTALS")
    validate_identity("SELECT INTERVAL t.days DAY")
    validate_identity("SELECT match('abc', '([a-z]+)')")
    validate_identity("SELECT window_start, avg(price) AS avg_price FROM tumble(coinbase, 10s) GROUP BY window_start")
