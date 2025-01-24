## Timeplus Connect

This project provides Python connector to interact with Proton, the code is based on [clickhouse-connect](https://github.com/ClickHouse/clickhouse-connect)

A high performance core database driver for connecting Timeplus to Python, Pandas, and Superset

* Pandas DataFrames
* Numpy Arrays
* PyArrow Tables
* Superset Connector
* SQLAlchemy 1.3 and 1.4 (limited feature set)

Timeplus Connect currently uses the Timeplus HTTP interface for maximum compatibility.

### Installation

```
pip install timeplus-connect
```

Timeplus Connect requires Python 3.8 or higher.

### Superset Connectivity

Timeplus Connect is fully integrated with Apache Superset. Previous versions of Timeplus Connect utilized a
dynamically loaded Superset Engine Spec, but as of Superset v2.1.0 the engine spec was incorporated into the main
Apache Superset project and removed from clickhouse-connect in v0.6.0. If you have issues connecting to earlier
versions of Superset, please use clickhouse-connect v0.5.25.

When creating a Superset Data Source, either use the provided connection dialog, or a SqlAlchemy DSN in the form
`timeplusdb://{username}:{password}@{host}:{port}`.

### SQLAlchemy Implementation

Timeplus Connect incorporates a minimal SQLAlchemy implementation (without any ORM features) for compatibility with
Superset. It has only been tested against SQLAlchemy versions 1.3.x and 1.4.x, and is unlikely to work with more
complex SQLAlchemy applications.

### Asyncio Support

Timeplus Connect provides an async wrapper, so that it is possible to use the client in an `asyncio` environment.
See the [run_async example](./examples/run_async.py) for more details.
