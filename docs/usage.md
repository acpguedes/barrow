# Usage

`barrow` operates on tabular data using a pipeline of commands connected with standard Unix pipes.

## Basic Concepts
Each command reads from `STDIN` or a file and writes to `STDOUT` or a file. Formats are inferred from file extensions or may be specified explicitly with `--input-format` and `--output-format`.

## Advanced Examples
### Join and Aggregate
```bash
barrow join id id --right other.csv --input data.csv --input-format csv --right-format csv | \
barrow mutate "total=price*qty" | \
barrow groupby category | \
barrow summary "revenue=sum(total)" --output-format parquet --output report.parquet
```
This pipeline joins two datasets on `id`, computes a new column, groups by `category`, and writes aggregated revenue to a Parquet file.
When writing grouped data to CSV, grouping information is stored in a leading
comment line of the form `# grouped_by: col1,col2` and is restored on read.

### Streaming Filters and Projections
```bash
barrow filter "score > 80" --input-format csv | \
barrow select "name,score" | \
barrow sort "score" --descending --output-format parquet --output top.parquet
```
Use streaming operations to filter, project, and sort large datasets without loading them entirely into memory.

## Performance Tips
- Prefer the Parquet format for large datasets to leverage Arrow's columnar layout.
- Provide explicit `--input-format` and `--output-format` to avoid format detection overhead.
- Use `select` early in pipelines to reduce the number of processed columns.
- When possible, install DuckDB and Arrow libraries with SIMD support for better throughput.
