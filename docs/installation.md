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

## Using the setup script
To create an isolated development environment with shell completion, run:
```bash
source scripts/setup_env.sh
```
This creates a virtual environment in `.venv`, installs development dependencies and enables `barrow` tab completion for the current shell.

## Running with Docker
A minimal Dockerfile is provided to run `barrow` in a container:
```bash
docker build -t barrow .
docker run --rm -it barrow --help
```

## Verifying the Installation
After installation, verify that the CLI works:
```bash
barrow --help
```
The command should print the top-level help screen describing available subcommands.
