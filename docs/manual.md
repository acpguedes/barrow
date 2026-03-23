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

## sort
Sort rows by column values.

```
barrow sort COLUMNS [options]
```

COLUMNS: comma-separated column names to sort by.

- `--desc`: sort in descending order.

```
barrow sort 'name,age' -i people.csv
```

```
barrow sort 'age' --desc -i people.csv -o sorted.csv
```

## sql
Execute a SQL query.

```
barrow sql QUERY [options]
```

QUERY: SQL query where the input table is named `tbl`.

```
barrow sql 'SELECT name, age FROM tbl WHERE age > 30' -i people.csv
```

```
barrow sql 'SELECT grp, SUM(a) AS total FROM tbl GROUP BY grp' -i data.csv
```

## window
Apply window functions.

```
barrow window ASSIGNMENTS [--by COLS] [--order-by COLS] [options]
```

ASSIGNMENTS: comma-separated `NAME=EXPR` pairs.

- `--by`: partition columns (comma-separated).
- `--order-by`: order columns within partitions (comma-separated).

```
barrow window 'rn=row_number()' --by grp --order-by val -i data.csv
```

```
barrow window 'ma=rolling_mean(value, 3)' --order-by date -i timeseries.csv
```

## explain
Show execution plan.

```
barrow explain COMMAND [EXPRESSION] [options]
```

COMMAND: the command to explain (filter, select, sort, etc.).
EXPRESSION: the expression or columns for that command.

```
barrow explain filter 'age > 30' -i people.csv
```

## Benchmark de comandos

Para comparar operações individuais e pipelines completos, use `scripts/benchmark.sh`. O script cria automaticamente arquivos CSV de teste pequenos, médios e grandes, mede variações em fases explícitas de `cold`, `warmup` e `hot`, adiciona equivalentes em SQL para as operações básicas quando fizer sentido e registra os tempos detalhados em `results.tsv`, além de gerar `summary.md` e `summary.json` no diretório de trabalho escolhido.

```bash
scripts/benchmark.sh --datasets small,medium --iterations 5
```

Se quiser focar em grupos específicos, use `--only`, por exemplo `--only filter,sql,pipeline`. Use `--warmup` para controlar quantas execuções preparam a fase hot, `--cold-runs` para repetir a fase cold e `--cleanup` para remover os artefatos gerados ao final mantendo os resultados e summaries.
