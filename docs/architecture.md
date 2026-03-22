# Architecture

## Purpose

`barrow` is evolving from a collection of Arrow-powered CLI subcommands into a compact analytical engine for tabular pipelines. The final architecture should preserve the current Unix-style ergonomics while adding a single execution model that can support CLI commands, SQL, future Python APIs, and optimized execution across multiple backends.

This document defines the target architecture for that final optimized version.

## Product goals

### Primary goals

- Keep the CLI fast, scriptable, and composable with `STDIN`/`STDOUT`.
- Support a unified transformation model across CSV, Parquet, Feather, and ORC.
- Expose the same engine through multiple frontends: CLI first, SQL next, Python API later.
- Optimize common pipelines through planning instead of executing each command eagerly.
- Use Apache Arrow as the in-memory interoperability layer.
- Use DuckDB selectively for SQL, complex joins, sorting, and advanced window execution.

### Non-goals

- Building a distributed query engine.
- Replacing general-purpose data warehouses.
- Supporting arbitrary Python execution inside user expressions.

## Architectural principles

1. **One engine, many frontends.** CLI, SQL, and future APIs must compile into the same logical plan.
2. **Lazy internally, explicit externally.** Users keep the current CLI experience, but execution is deferred until the engine has a full plan to optimize.
3. **Arrow-native contracts.** Arrow remains the canonical in-memory table format between layers.
4. **Backend polymorphism.** The planner chooses the best execution backend for each plan or plan fragment.
5. **Metadata is logical state, not transport state.** Grouping, ordering, and partition semantics should live in the logical plan rather than only in schema metadata.
6. **Format adapters are isolated.** File-format detection, scan behavior, schema inference, and sink writing must remain outside core planning logic.

## Current-state summary

The current codebase already has the right seeds for the target design:

- A thin CLI layer with subcommands for `filter`, `select`, `mutate`, `groupby`, `summary`, `ungroup`, `join`, and `view`.
- A separate `operations` package that contains transformation primitives.
- `io.reader` and `io.writer` helpers that detect and serialize Arrow-compatible formats.
- An expression parser/evaluator used by row filtering and column mutation.
- Early support for `window` and `sql` operations in the operations layer, even though they are not yet fully surfaced as first-class CLI frontends.

The final architecture formalizes and generalizes this structure.

## Target architecture overview

```text
+-----------------------------+
| Frontends                   |
| CLI | SQL | Python API      |
+-------------+---------------+
              |
              v
+-----------------------------+
| Frontend Adapters           |
| cli_to_plan | sql_to_plan   |
| api_to_plan                 |
+-------------+---------------+
              |
              v
+-----------------------------+
| Core Logical Layer          |
| LogicalPlan | Expr AST      |
| Schema | LogicalProperties  |
+-------------+---------------+
              |
              v
+-----------------------------+
| Optimizer                   |
| pushdown | fusion |         |
| simplification | backend    |
+-------------+---------------+
              |
              v
+-----------------------------+
| Execution Engine            |
| planner | scheduler |       |
| materialization policy      |
+------+------+---------------+
       |      |
       v      v
+----------+  +---------------+
| Arrow    |  | DuckDB        |
| backend  |  | backend       |
+----------+  +---------------+
       \         /
        \       /
         v     v
      +-----------------------+
      | I/O Adapters          |
      | CSV | Parquet |       |
      | Feather | ORC         |
      +-----------------------+
```

## Layer-by-layer design

### 1. Frontends

Frontends are responsible only for turning user input into a common internal representation.

#### CLI frontend

The CLI remains the flagship interface, but it stops executing operations directly. Instead, each subcommand builds a logical operator and appends it to a plan.

Planned CLI capabilities:

- Existing commands: `filter`, `select`, `mutate`, `groupby`, `summary`, `ungroup`, `join`, `view`
- New first-class commands: `sql`, `window`, `sort`, `limit`, `explain`
- Shared I/O options with consistent semantics
- Pipeline awareness so chained commands can become one optimized logical graph

#### SQL frontend

SQL should become a first-class input mode, not just a direct DuckDB call. The preferred design is:

- Parse SQL into a logical plan when possible.
- Fall back to a DuckDB-backed physical plan for unsupported cases.
- Preserve output contracts with the rest of the engine.

#### Python API frontend

A future Python API should expose fluent lazy operations such as:

```python
(
    barrow.scan("data.parquet")
    .filter("a > 1")
    .mutate("c=a+b")
    .groupby(["grp"])
    .summary({"c": "sum"})
    .collect()
)
```

The API should be syntactic sugar over the same logical-plan primitives used by the CLI.

### 2. Frontend adapters

This layer translates frontend-specific constructs into the core logical model.

Suggested modules:

- `frontend/cli_to_plan.py`
- `frontend/sql_to_plan.py`
- `frontend/python_api.py`

Responsibilities:

- Normalize names, aliases, and option defaults.
- Validate command-specific syntax.
- Produce core `LogicalPlan` nodes.
- Avoid data access and execution decisions.

### 3. Core logical layer

This is the heart of the target architecture.

#### Core entities

- `LogicalPlan`: immutable tree or DAG representing the requested transformations.
- `LogicalNode`: base type for all plan nodes.
- `Schema`: columns, dtypes, nullability, and optional source lineage.
- `LogicalProperties`: grouping, ordering, partitioning, row-count estimates, and format hints.
- `Expression AST`: typed internal representation for user expressions.

#### Canonical logical nodes

- `Scan`
- `Project`
- `Filter`
- `Mutate`
- `Aggregate`
- `Join`
- `Window`
- `Sort`
- `Limit`
- `Ungroup`
- `View`
- `Sink`

#### Why this layer matters

Today, grouping is represented through schema metadata when tables are written and read back. In the target architecture, grouping becomes a logical property of the plan. That allows the optimizer and execution engine to reason about semantics without depending on transport-specific metadata.

### 4. Expression subsystem

The current parser is a good start, but the final system should split expression handling into three steps:

1. **Parse** user text into an AST.
2. **Analyze** names, function usage, and types.
3. **Compile** expressions into backend-specific execution forms.

#### Design goals

- Keep the current approachable expression syntax.
- Support column references, arithmetic, comparisons, boolean logic, and a curated function set.
- Compile simple expressions to Arrow compute kernels when possible.
- Compile advanced expressions to DuckDB SQL when beneficial.
- Produce stable, user-friendly diagnostics.

Suggested modules:

- `expr/parser.py`
- `expr/analyzer.py`
- `expr/compiler_arrow.py`
- `expr/compiler_duckdb.py`
- `expr/types.py`

### 5. Optimizer

The optimizer is the main performance lever in the final version.

#### Required rule families

##### Projection pushdown

Read only the columns needed downstream, especially for Parquet and ORC scans.

##### Filter pushdown

Push predicates into scans and backend-native query execution whenever possible.

##### Operator fusion

Collapse adjacent transforms such as:

- `select` + `mutate`
- multiple `mutate` expressions
- redundant projections
- unnecessary materializations

##### Logical simplification

Remove no-op or redundant nodes such as:

- `ungroup` on ungrouped data
- repeated sorts on identical keys
- `select` that preserves all columns in order

##### Backend selection

Choose the right physical backend per plan or plan fragment.

Examples:

- Arrow backend for vectorized projection/filter/mutate
- DuckDB backend for heavy joins, SQL, sort-heavy plans, and complex window logic

##### Materialization policy

Decide when to keep data lazy, when to stream batches, and when to materialize full tables.

### 6. Execution engine

The execution engine transforms optimized logical plans into executable physical plans.

#### Responsibilities

- Build physical operators from logical nodes.
- Pick backends and split the plan into backend-compatible fragments.
- Manage intermediate representation boundaries.
- Guarantee stable output contracts such as `pa.Table` or a standardized `ExecutionResult`.

#### Recommended abstraction

```text
LogicalPlan -> OptimizedPlan -> PhysicalPlan -> ExecutionResult
```

#### ExecutionResult contract

Use a normalized result wrapper with helpers such as:

- `to_table()`
- `to_batches()`
- `write_to_sink()`
- `schema`
- `stats`

This prevents backend-specific return types from leaking into tests and callers.

### 7. Backend layer

#### Arrow backend

Use Arrow-native execution for:

- scans and sinks
- `select`
- `filter`
- `mutate`
- simple aggregations
- lightweight transformations on in-memory tables

Advantages:

- tight integration with current code
- minimal overhead
- predictable vectorized execution

#### DuckDB backend

Use DuckDB for:

- SQL frontends and SQL fallbacks
- large joins
- ordering and sorting
- advanced window functions
- plans where relational optimization is materially better than custom execution

#### Hybrid execution

In the final optimized version, the planner should be allowed to run a plan partly in Arrow and partly in DuckDB, with Arrow Tables as the interchange contract.

### 8. I/O adapters

I/O should remain isolated from planning and transformation logic.

#### Responsibilities

- format detection
- schema inference
- scan options
- sink configuration
- transport details for files, pipes, and future remote storage

#### Suggested package structure

```text
io/
  scan/
    csv.py
    parquet.py
    feather.py
    orc.py
  sink/
    csv.py
    parquet.py
    feather.py
    orc.py
  formats.py
  options.py
```

#### Future extension points

- Arrow IPC streams for process-to-process transfer
- directory-based dataset scans
- remote object storage adapters
- partition discovery

## Data and metadata model

### Current limitation

Grouping metadata is currently embedded in schema metadata when transporting grouped tables through CSV or Arrow formats.

### Target model

Promote execution semantics into a dedicated logical-properties model.

Suggested logical properties:

- `group_keys`
- `ordering`
- `partitions`
- `source_format`
- `estimated_rows`
- `estimated_size_bytes`
- `is_materialized`

Transport metadata may still exist for interoperability, but it should never be the only source of truth for plan semantics.

## Performance strategy

### 1. Lazy planning by default

Even when invoked from a CLI pipeline, `barrow` should attempt to build a larger logical plan before executing.

### 2. Batch-aware execution

Support chunked processing for operations that do not require full materialization.

Good streaming candidates:

- `select`
- `filter`
- some `mutate` operations
- format conversion

Materialization-required operators:

- global sort
- many joins
- many window functions
- some grouped aggregations depending on backend strategy

### 3. Column pruning

Apply column pruning as early as possible to reduce memory pressure and I/O cost.

### 4. Backend-aware optimization

Let the optimizer treat backend capabilities as part of the planning problem.

### 5. Stable interchange format

Use Arrow Tables or Arrow RecordBatches as the normalized interchange representation between fragments.

## Error-handling model

The final architecture should use typed error domains:

- `FrontendError`
- `ExpressionError`
- `PlanningError`
- `OptimizationError`
- `ExecutionError`
- `IOError`

Benefits:

- clearer diagnostics
- better testability
- easier CLI messaging
- easier API integration

## Observability and explainability

To make the engine debuggable and trustworthy, add:

- `barrow explain ...` to print logical and physical plans
- execution stats per operator
- backend choice annotations
- optional timing and row-count output
- debug logging hooks around optimization decisions

## Testing strategy for the target architecture

### Unit tests

- expression parsing and semantic analysis
- optimizer rule behavior
- backend adapters
- logical-plan validation

### Integration tests

- end-to-end CLI pipelines
- mixed-format pipelines
- SQL and window workflows
- cross-backend equivalence

### Golden tests

- plan rendering for `explain`
- stable error messages
- documentation examples

### Compatibility tests

- verify backend return types normalize correctly
- verify Arrow/DuckDB version differences do not leak into public behavior

## Suggested target package layout

```text
barrow/
  cli/
    main.py
    commands/
      filter.py
      select.py
      mutate.py
      groupby.py
      summary.py
      join.py
      window.py
      sql.py
      view.py
      explain.py
  frontend/
    cli_to_plan.py
    sql_to_plan.py
    python_api.py
  core/
    plan.py
    nodes.py
    schema.py
    properties.py
    result.py
    errors.py
  optimizer/
    optimizer.py
    rules/
      projection_pushdown.py
      filter_pushdown.py
      fusion.py
      simplify.py
      backend_selection.py
  execution/
    engine.py
    physical_plan.py
    backends/
      arrow_backend.py
      duckdb_backend.py
      streaming_backend.py
  expr/
    parser.py
    analyzer.py
    compiler_arrow.py
    compiler_duckdb.py
    types.py
  io/
    scan/
      csv.py
      parquet.py
      feather.py
      orc.py
    sink/
      csv.py
      parquet.py
      feather.py
      orc.py
    formats.py
    options.py
```

## Migration plan

### Phase 1: stabilize contracts

- Introduce `ExecutionResult`.
- Normalize SQL backend return types.
- Separate frontend parsing from execution entrypoints.
- Add architecture tests around result contracts.

### Phase 2: introduce logical planning

- Create `LogicalPlan` and initial nodes.
- Convert CLI commands to plan builders.
- Keep current eager execution behind the engine as an interim implementation.

### Phase 3: add optimization

- Implement projection pushdown.
- Implement filter pushdown.
- Add operator fusion.
- Add backend selection rules.

### Phase 4: expose new frontends

- Add `sql` CLI command backed by the common engine.
- Add `window` CLI command through the planner.
- Add `explain` command.
- Add optional Python API.

### Phase 5: advanced execution

- Add chunked execution where possible.
- Add dataset scans and partition-aware reads.
- Add richer observability and plan stats.

## Recommended immediate next steps

1. Create a `core.result.ExecutionResult` abstraction.
2. Refactor the CLI so subcommands build plan objects instead of directly reading, executing, and writing.
3. Move grouping semantics from implicit schema metadata to explicit logical properties.
4. Introduce a minimal optimizer with projection and filter pushdown.
5. Add a backend selection layer that chooses Arrow or DuckDB.
6. Expose `sql` and `window` only after they run through the same execution contract as the rest of the engine.

## Final recommendation

The ideal final version of `barrow` is not just a collection of CLI subcommands. It is a lightweight analytical engine with:

- one logical model,
- one optimization pipeline,
- multiple frontends,
- multiple execution backends,
- and Arrow as the interoperability spine.

That architecture keeps the current usability of the project while giving it a credible path toward better performance, cleaner extensibility, and a more coherent product surface.
