# Manual

`barrow` provides subcommands for working with tabular data. Each command can read from `STDIN` or a file and write to `STDOUT` or a file.

## Common I/O options

- `-i`, `--input PATH` – input file. Reads `STDIN` if omitted.
- `--input-format {csv,parquet,feather,orc}` – format of the input file.
- `-o`, `--output PATH` – output file. Writes to `STDOUT` if omitted.
- `--output-format {csv,parquet,feather,orc}` – format of the output. Defaults to the input format and is ignored by `view`.
- `--csv`, `--parquet`, `--feather`, `--orc` – shortcut flags to set the output format.
- `--delimiter CHAR` – field delimiter for CSV input; also used for output unless `--csv-out-delimiter` is given.
- `--csv-out-delimiter CHAR` – field delimiter for CSV output.
- `--tmp` – write intermediate results to Feather when using pipes for faster processing.

## filter
Filter rows using a boolean expression.

```
barrow filter "score > 80" -i data.csv -o filtered.csv
```

## select
Select a comma-separated list of columns.

```
barrow select "name,score" -i data.csv -o subset.csv
```

## mutate
Add or modify columns with `NAME=EXPR` pairs.

```
barrow mutate "total=price*qty" -i sales.csv -o extended.csv
```

## groupby
Group rows by columns.

```
barrow groupby category -i sales.csv | barrow summary "revenue=sum(total)"
```

Grouped data keeps track of the grouping so that subsequent operations like `summary` can aggregate correctly.

## summary
Aggregate a grouped table with `COLUMN=AGG` pairs.

```
barrow groupby category -i sales.csv | barrow summary "revenue=sum(total)"
```

## ungroup
Remove grouping metadata.

```
barrow groupby category -i data.csv | barrow ungroup
```

## join
Join two tables on key columns.

```
barrow join id id --right other.csv -i left.csv -o joined.csv
```

Additional options:

- `--right PATH` – right input file.
- `--right-format {csv,parquet,feather,orc}` – format of the right file.
- `--join-type {inner,left,right,outer}` – type of join (default `inner`).

## view
Display a table in CSV format to `STDOUT` for inspection.

```
barrow view -i data.parquet
```

`view` accepts `--output-format` for API compatibility but always writes CSV to `STDOUT`.
