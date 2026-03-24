# barrow
A Bash tool for data manipulation using tabular formats, based on Apache Arrow.
It supports common data operations like select, filter, mutate, groupby, summary, ungroup, join, sort, sql, window, and explain.
Commands read from files or `STDIN` and write to files or `STDOUT` in CSV, Parquet,
Feather, or ORC format.

## Installation
```bash
pip install barrow
```

For a development install with linting and testing tools:

```bash
make install
```

`make install` installs development dependencies and configures pre-commit hooks.

To create an isolated environment with autocompletion, run:
```bash
source scripts/setup_env.sh
```
A minimal Dockerfile is available for container usage:
```bash
docker build -t barrow .
docker run --rm barrow --help
```

The Makefile also provides common tasks:

```bash
make lint    # run linters via pre-commit
make format  # format code via pre-commit
make test    # run the test suite
make clean   # remove build artifacts
```

For quick CLI performance comparisons, use `scripts/benchmark.sh`. The script generates deterministic CSV fixtures automatically, supports optional `tiny` and `xlarge` datasets in addition to the default `small`, `medium`, and `large` sizes, runs representative commands and pipelines across explicit `cold`, `warmup`, and `hot` phases, uses the same `--iterations` run count for every enabled phase of a variant, includes SQL equivalents for the basic operations where they make sense, captures wall time plus peak RSS and CPU time, and saves detailed timings to `results.tsv` plus final summaries in `summary.md` and `summary.json` inside the chosen workspace.

```bash
scripts/benchmark.sh --datasets small,medium --iterations 5
scripts/benchmark.sh --datasets tiny,xlarge --only filter,pipeline --iterations 2
```

## Usage
All subcommands accept `--input`/`-i`, `--input-format`, `--output`/`-o`, and
`--output-format` to control I/O. These options support `csv`, `parquet`,
`feather`, or `orc`. Convenience flags `--csv`, `--parquet`, `--feather`, and
`--orc` set the output format directly. Use `--delimiter` to specify the field
delimiter for CSV input and output, or `--csv-out-delimiter` to choose a
different delimiter for CSV output. `--tmp` writes Feather to pipes for faster
intermediate processing. When omitted, formats are inferred from file extensions
or magic bytes when reading from `STDIN`. Leaving out `--input` makes the
command read from `STDIN`; omitting `--output` writes to `STDOUT`. If
`--output-format` is not given, the command writes using the input format.

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
# filter outputs CSV by default; select uses the --parquet shortcut
barrow filter "a > 1" --input data.csv | \
barrow select "b,grp" --parquet --output result.parquet
```

### Mutate → groupby → summary
```bash
# mutate and groupby use Feather pipes with --tmp; summary outputs Parquet
barrow mutate "c=a+b" --input data.csv --tmp | \
barrow groupby grp --tmp | \
barrow summary "c=sum" --parquet --output out.parquet
```

### Custom delimiters
```bash
# read tab-separated input and write semicolon-separated output
barrow select "a,b" --input data.tsv --delimiter '\t' --csv-out-delimiter ';' \
  --output result.csv
```

### Filter → mutate → select → groupby → summary
```bash
# filter, mutate, select, groupby, and summarize
barrow filter "a > 1" --input data.csv --input-format csv | \
barrow mutate "c=a+b" | \
barrow select "c,grp" | \
barrow groupby grp | \
barrow summary "c=sum"
```

```csv
grp,c_sum
x,7
y,9
```

Note: Writing grouped data to CSV drops grouping metadata; use Parquet to preserve it.

These pipelines demonstrate reading from `STDIN` and writing to `STDOUT` while combining operations with pipes.

### sort
`barrow sort COLUMNS [--desc]`
: Sort rows by column values.

### sql
`barrow sql QUERY`
: Execute a SQL query where the input table is named `tbl`.

### window
`barrow window ASSIGNMENTS [--by COLS] [--order-by COLS]`
: Apply window functions with optional partitioning and ordering.

### explain
`barrow explain COMMAND [EXPRESSION]`
: Show the execution plan for a command without running it.

### Sort, SQL, window, and explain examples
```bash
# sort by age descending
barrow sort 'age' --desc -i people.csv -o sorted.csv

# run a SQL query
barrow sql 'SELECT name, age FROM tbl WHERE age > 30' -i people.csv

# add a row number with window function
barrow window 'rn=row_number()' --by grp --order-by val -i data.csv

# inspect the execution plan
barrow explain filter 'age > 30' -i people.csv
```

## Roadmap
- ~~Support window functions~~ (implemented)
- ~~Provide a SQL interface~~ (implemented)
- Active projection/filter pushdown in scans
- Lazy execution mode
- Backend selection based on cost estimation

## Testing
Run the test suite with:

```bash
make test
```

Optional dependencies:
- `argcomplete` for shell auto-completion (`pip install argcomplete`).
