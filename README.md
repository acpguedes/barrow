# barrow
A Bash tool for data manipulation using tabular formats, based on Apache Arrow.
It supports common data operations like select, filter, mutate, groupby, and summary, reading
from files or `STDIN` and writing to files or `STDOUT` in CSV or Parquet format.

## Roadmap
- Support window functions
- Provide a SQL interface

## Command line usage

```
barrow filter "a > 1" --input data.csv --output result.parquet
barrow join id id --input left.csv --right other.parquet --output out.csv
# membership and pattern matching
barrow filter "country in ['US', 'CA'] and name like 'Jo%'" --input data.csv --output out.csv
```

`--input-format` and `--output-format` (and `--right-format` for joins) accept
`csv` or `parquet`. When omitted, the format is inferred from the file extension
or, when reading from `STDIN`, from the file's magic bytes.

## Running tests

Run the test suite with:

```bash
pytest
```
