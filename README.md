## Timeplus Connect

This project provides Python connector to interact with [Timeplus Enterprise](https://www.timeplus.com/product) or [Timeplus Proton](https://github.com/timeplus-io/proton). The code is based on [clickhouse-connect](https://github.com/ClickHouse/clickhouse-connect).

A high performance database driver for connecting Timeplus to Python, Pandas, and Superset

* Pandas DataFrames
* Numpy Arrays
* PyArrow Tables
* Superset Connector
* SQLAlchemy 1.3 and 1.4 (limited feature set)

Timeplus Connect currently uses the Timeplus HTTP interface for maximum compatibility, defaulting to 8123.

### Installation

```
pip install timeplus-connect
```

Timeplus Connect requires Python 3.9 or higher.

### Superset Connectivity

Timeplus Connect is fully integrated with Apache Superset.

When creating a Superset Data Source, use a SqlAlchemy DSN in the form
`timeplus://{username}:{password}@{host}:{port}`, such as `timeplus://default:password@localhost:8123`.

### SQLAlchemy Implementation

Timeplus Connect incorporates a minimal SQLAlchemy implementation (without any ORM features) for compatibility with
Superset. It has only been tested against SQLAlchemy versions 1.3.x and 1.4.x, and is unlikely to work with more
complex SQLAlchemy applications.

### Asyncio Support (to be verified)

Timeplus Connect provides an async wrapper, so that it is possible to use the client in an `asyncio` environment.
See the [run_async example](./examples/run_async.py) for more details.
