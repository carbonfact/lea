# lea

lea is an opinionated alternative to [dbt](https://www.getdbt.com/), [SQLMesh](https://sqlmesh.com/), and [Dataform](https://cloud.google.com/dataform).

- [x] Automatic DAG construction
- [x] CLI
- [x] Singular testing
- [x] DAG selection
- [x] Checkpointing
- [x] Schema and sub-schema handling
- [x] Dry runs
- [x] Write in Python and/or SQL
- [x] Jinja templating
- [x] Development schemas
- [ ] Expansion (works, but hacky)
- [x] Documentation
- [x] Mermaid visualization
- [x] Asynchronous running
- [x] Incremental reruns
- [ ] Linting
- [ ] Data lineage
- [ ] Importable as a library
- [ ] Multi-engine support (only BigQuery for now)
- [x] Historization
- [ ] Only refresh what changed, based on git

## Usage

### `lea run`

```sh
lea run ./views
lea run ./views --dry
lea run ./views --only core.measure_carbonverses
```

### `lea test`

```sh
lea test ./views
```

### `lea export`

```sh
lea export ./views
```

### `lea archive`

```sh
lea archive ./views kpis.all
```

### `lea docs`

```sh
lea docs ./views --output-dir ./docs
```
