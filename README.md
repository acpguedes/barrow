# barrow
A Bash tool for data manipulation using tabular formats, based on Apache Arrow.
It supports common data operations like select, filter, mutate, groupby, and summary, reading
from files or `STDIN` and writing to files or `STDOUT` in CSV or Parquet format.

## Roadmap
- Support joins
- Support window functions
- Provide a SQL interface

## Command line usage

```
barrow --input data.csv --input-format csv --output result.parquet --output-format parquet
```

`--input-format` and `--output-format` accept `csv` or `parquet`. When omitted, the
defaults are CSV for input and Parquet for output.

## Running tests

Run the test suite with:

```bash
pytest
```
