# Installation

## Prerequisites
- Python 3.11 or later
- [DuckDB](https://duckdb.org/) and [Apache Arrow](https://arrow.apache.org/) are required. They are installed automatically when using `pip` but can also be installed system wide for optimal performance.

## Install from PyPI
```bash
pip install barrow
```
This command installs `barrow` along with its core dependencies such as `pyarrow`, `duckdb` and `numpy`.

## Development Install
For contributing or running the test suite, install the optional development and documentation dependencies:
```bash
pip install -e .[dev,docs]
```
This installs linting tools, test runners and documentation tooling like MkDocs.

## Verifying the Installation
After installation, verify that the CLI works:
```bash
barrow --help
```
The command should print the top-level help screen describing available subcommands.
