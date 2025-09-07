# barrow
A Bash tool for data manipulation using tabular formats, based on Apache Arrow.
## TODO
- Create project structure
- Implement STDIN
- Implement STDOUT.
- Implement error class
- Implement syntax to get expressions from option
- Implement select
- Implement filter
- Implement mutate
- Implement groupby
- Implement summary

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
