# barrow
A Bash tool for data manipulation using tabular formats, based on Apache Arrow.
It supports common data operations like select, filter, mutate, groupby, summary, ungroup, and join.
Commands read from files or `STDIN` and write to files or `STDOUT` in CSV or Parquet format.

## Installation
```bash
pip install barrow
```

For a development install with linting and testing tools:

```bash
pip install -e .[dev]
```

## Usage
All subcommands accept `--input`/`-i`, `--input-format`, `--output`/`-o`, and `--output-format` to control I/O. These options support `csv` or `parquet`. When omitted, formats are inferred from file extensions or magic bytes when reading from `STDIN`. Leaving out `--input` makes the command read from `STDIN`; omitting `--output` writes to `STDOUT`.

### filter
`barrow filter EXPRESSION`
: Filter rows by a boolean expression.

### select
`barrow select COLUMN[,COLUMN...]`
: Select specific columns.

### mutate
`barrow mutate NAME=EXPR[,NAME=EXPR...]`
: Add or modify columns using expressions.

### groupby
`barrow groupby COLUMN[,COLUMN...]`
: Group rows by one or more columns.

### summary
`barrow summary COLUMN=AGG[,COLUMN=AGG...]`
: Aggregate a grouped table using functions like `sum`, `mean`, or `count`.

### ungroup
`barrow ungroup`
: Remove grouping metadata from a table.

### join
`barrow join LEFT_KEY RIGHT_KEY --right FILE [--join-type TYPE]`
: Join two tables. `--join-type` accepts `inner`, `left`, `right`, or `outer`; `--right-format` chooses the right table's format.

## Examples
### Filter then select
```bash
barrow filter "a > 1" --input data.csv | \
barrow select "b,grp" --output result.csv
```

### Mutate → groupby → summary
```bash
barrow mutate "c=a+b" --input data.csv | \
barrow groupby grp | \
barrow summary "c=sum" --output out.parquet
```

These pipelines demonstrate reading from `STDIN` and writing to `STDOUT` while combining operations with pipes.

## Roadmap
- Support window functions
- Provide a SQL interface

## Testing
Run the test suite with:

```bash
pytest
```

Optional dependencies:
- `argcomplete` for shell auto-completion (`pip install argcomplete`).
