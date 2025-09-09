# Usage

`barrow` operates on tabular data using a pipeline of commands connected with standard Unix pipes.

## Basic Concepts
Each command reads from `STDIN` or a file and writes to `STDOUT` or a file. Formats are inferred from file extensions or may be specified explicitly with `--input-format` and `--output-format`.

## Advanced Examples
### Join and Aggregate with ORC Output
```bash
barrow join id id --right other.csv --input data.csv --input-format csv --right-format csv --delimiter ';' | \
barrow mutate "total=price*qty" --tmp | \
barrow groupby category --tmp | \
barrow summary "revenue=sum(total)" --orc --output report.orc
```
This pipeline joins two semicolon-delimited CSV datasets on `id`, uses `--tmp` to store intermediate results in Feather, groups by `category`, and writes aggregated revenue to an ORC file.
When writing grouped data to CSV, grouping information is stored in a leading
comment line of the form `# grouped_by: col1,col2` and is restored on read.

### Streaming Filters and Projections
```bash
barrow filter "score > 80" --input-format csv | \
barrow select "name,score" | \
barrow sort "score" --descending --output-format parquet --output top.parquet
```
Use streaming operations to filter, project, and sort large datasets without loading them entirely into memory.

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

### Inspecting Results
When working with binary formats like Parquet, append `view` to a pipeline to
inspect the data in a human-readable form:

```bash
barrow summary "revenue=sum(total)" --output-format parquet | \
barrow view
```

`view` ignores `--output-format` and always prints text (CSV) to `STDOUT`. It
can also read directly from a file:

```bash
barrow view -i data.parquet
```

## Performance Tips
- Prefer the Parquet format for large datasets to leverage Arrow's columnar layout.
- Provide explicit `--input-format` and `--output-format` to avoid format detection overhead.
- Use `select` early in pipelines to reduce the number of processed columns.
- When possible, install DuckDB and Arrow libraries with SIMD support for better throughput.
