<p align="center">
  <img src="docs/logo.png" alt="lakemon" width="200">
</p>

<p align="center">
  <a href="https://github.com/paulosuzart/lakemon-duckdb-ext/actions/workflows/PolicyTests.yml"><img src="https://github.com/paulosuzart/lakemon-duckdb-ext/actions/workflows/PolicyTests.yml/badge.svg?branch=main" alt="Policy tests"></a>
  <a href="https://github.com/paulosuzart/lakemon-duckdb-ext/actions/workflows/MainDistributionPipeline.yml"><img src="https://github.com/paulosuzart/lakemon-duckdb-ext/actions/workflows/MainDistributionPipeline.yml/badge.svg?branch=main" alt="Main Extension Distribution Pipeline"></a>
</p>

# lakemon

DuckDB extension for DuckLake maintenance.

**Maintain a DuckLake from SQL: expire old snapshots, rewrite files with deleted rows (worst first, in byte-weighted steps), and merge small files into right-sized ones.** One `CALL` per table pass, one `CALL` for catalog retention.

```sql
CALL lakemon_maintain('lake');         -- flush, rewrite deleted bytes, merge small files
CALL lakemon_maintain_global('lake');  -- expire snapshots, clean up old and orphaned files
CALL lakemon_maintain('lake', dry_run => true);  -- plan only: one row per step, including the SQL
```

## Why

A DuckLake catalog that sees steady updates, deletes, and small appends drifts in three ways: delete files pile up on top of data files, many small Parquet files accumulate, and old snapshots keep files alive. DuckLake ships the native `CALL`s to fix each of these, and `CHECKPOINT` runs them all at once. What it does not give you is a decision about *which* files deserve the work, a way to slice a large rewrite into bounded steps, or a per-step record of what happened.

lakemon is that decision layer. It reads DuckLake metadata, plans the work, and then calls the same native DuckLake functions:

- **Rewrites the worst files first.** Files are ranked by delete fraction and cut into up to `max_rewrite_steps` (default `3`) `ducklake_rewrite_data_files` `CALL`s by cumulative bytes, so the most-deleted data is rewritten first and no single `CALL` has to rewrite every dirty file.
- **Merges by size band.** Micro, small, and medium files are compacted separately, each with its own `target_file_size`.
- **Splits table work from catalog retention.** `lakemon_maintain` handles tables (all of them, or one); `lakemon_maintain_global` handles expire and cleanup once per catalog.
- **Shows its plan.** `dry_run => true` returns one row per step (`status = planned` plus the generated SQL) without touching files. The same `CALL` without `dry_run` plans and runs those steps.
- **Respects your catalog.** Native DuckLake options (`rewrite_delete_threshold`, `target_file_size`, `expire_older_than`, `delete_older_than`, `auto_compact`) set via `set_option` override lakemon's defaults.

## Who it is for

- Teams running DuckLake catalogs with frequent deletes, updates, or small appends.
- Anyone scheduling maintenance from SQL (a cron job, an orchestrator task, a DuckDB script) who wants a plan they can inspect and a result they can log.
- Operators who need to bound maintenance work per table instead of running one large checkpoint.

lakemon does not replace the `ducklake` extension; it requires it.

## Quick start

```sql
INSTALL ducklake;
LOAD ducklake;
INSTALL lakemon FROM community;
LOAD lakemon;

ATTACH 'ducklake:metadata.ducklake' AS lake (DATA_PATH 'data_files');

-- What does each table look like, and what would lakemon do?
CALL lakemon_table_stats('lake');

-- Plan only: returns the DuckLake CALLs without running them
CALL lakemon_maintain('lake', dry_run => true);
CALL lakemon_maintain_global('lake', dry_run => true);

-- Run it: table work first, then catalog retention
CALL lakemon_maintain('lake');
CALL lakemon_maintain_global('lake');
```

Single table:

```sql
CALL lakemon_maintain('lake', 'myschema.t');
CALL lakemon_maintain_global('lake');
```

## Requirements

- DuckDB **1.5.\*** (built and tested against `v1.5.5`) or DuckDB **2.x** (`v2.0-cyanoptera` / upcoming 2.0)
- The core **`ducklake`** extension loaded in the same session
- An attached DuckLake catalog (`ATTACH 'ducklake:...' AS lake`)

## Install / load

```sql
INSTALL ducklake;
LOAD ducklake;
INSTALL lakemon FROM community;
LOAD lakemon;
```

Until the community listing is merged, load the locally built artifact with unsigned extensions enabled (`duckdb -unsigned`):

```sql
LOAD 'build/release/extension/lakemon/lakemon.duckdb_extension';
```

See [community/README.md](community/README.md) for the remaining listing steps.

## How it works

`CALL` / `SELECT * FROM lakemon_maintain(...)` (and the `_global` form) **plan and execute** by default. `dry_run => true` returns the plan only.

Execute does not open a nested `Connection` from table-function **init** (that waits on the catalog/transaction lock held by the outer query and hangs). `bind_replace` plans, then runs each planned DuckLake `CALL` on a nested `Connection` so each statement commits on its own. DuckLake rejects a single transaction that both changes data (flush, `set_option`) and compact (merge / expire). `dry_run => false` is refused under `EXPLAIN` / `PREPARE` / `EXECUTE` / `DESCRIBE` / `SUMMARIZE` (those bind without a single execution, or rebind) and inside an explicit `BEGIN` (nested commits would wait on the outer catalog lock). An optional `PRAGMA lakemon_maintain(...)` uses the same plan-and-execute path as `CALL`.

### Table pass: `lakemon_maintain`

For each table (or the one you name), in order:

1. Catalog inventory (DuckLake metadata via `__ducklake_metadata_<catalog>`, or `ducklake_table_info` / `ducklake_list_files`)
2. `ducklake_flush_inlined_data`
3. `ducklake_rewrite_data_files`, once per rewrite rung, worst delete fraction first
4. `ducklake_merge_adjacent_files`, once per size band that has at least two files

After inventory succeeds, flush, rewrite, and merge are planned independently. Execute is fail-fast: a failed DuckLake step stops the rest of the batch.

### Catalog pass: `lakemon_maintain_global`

Once per catalog:

1. `ducklake_expire_snapshots` when `expire_older_than` is set
2. `ducklake_cleanup_old_files` + `ducklake_delete_orphaned_files`

Expire and cleanup are independent of each other and of the table pass.

### Rewrite ladder (byte-weighted, adaptive)

The rewrite plan is driven by bytes, not by fixed thresholds or delete-count buckets. lakemon does **not** use equal-width `delete_threshold` steps (`0.1`, `0.2`, …), equal-count chunks, or absolute `delete_count` high/medium/low bands.

1. Keep only files whose delete fraction is **at least `min_delete_ratio`** (default `0.01`). Files below that floor are excluded from the byte pool and from rung membership, so a huge near-clean file cannot collapse the ladder.
2. Order qualifying files by delete fraction, descending (worst first).
3. Walk cumulative `file_size_bytes` and cut a threshold each time the running total crosses `total_bytes * i/N`, where `N = max_rewrite_steps` (default `3`, range `1`–`16`).
4. Each cut becomes one `ducklake_rewrite_data_files` `CALL` with that data-driven `delete_threshold`. Duplicate cuts (same threshold) merge their file slices. Optional `byte_budget` can trim a rung after the cuts are chosen.

The last (most inclusive) threshold is the smallest observed delete fraction among qualifying files. A table with no qualifying delete data skips rewrite (flush and merge still run).

**lakemon never emits `delete_threshold` 0.0 or below.** A native threshold of `0` matches every file in the table, including files with no deletes. A rung whose threshold would be `<= 0` is skipped. If you want a zero threshold, call DuckLake directly:

```sql
CALL ducklake_rewrite_data_files('lake', 't', delete_threshold => 0)
```

Why slice: a single near-zero threshold would rewrite every dirty file in one `CALL` (time, memory, CPU). An early high-fraction rung rewrites the worst files first; later rungs lower the threshold as more bytes accumulate.

| Knob | Default | Meaning |
| --- | --- | --- |
| `max_rewrite_steps` | `3` (`1`–`16`) | How many byte-weighted cuts to emit |
| `min_delete_ratio` | `0.01` (`(0, 1]`) | Exclude files below this delete fraction from the ladder |
| `byte_budget` | unset | Optional per-rung size cap |

Delete fractions come from `delete_count` / `record_count` in DuckLake metadata. The `ducklake_list_files` fallback does not expose counts, so lakemon synthesizes a conservative fraction from `delete_file_size_bytes / file_size_bytes` (clamped to `(0, 1]`). Metadata inventory is still required for accurate count-based ladders.

**Catalog override.** When DuckLake `rewrite_delete_threshold` is set in **(0, 1]** (table → schema → global), lakemon emits **one** rewrite `CALL` with that value (full override of per-rung thresholds and CALL count) and keeps only files whose delete fraction is **>=** that value. If none remain, rewrite is skipped. If the catalog value is **<= 0**, rewrite is skipped for that table this cycle (flush and merge still run). `max_rewrite_steps` / `byte_budget` / `min_delete_ratio` have already been applied.

### Merge size bands

| Tier | File size | `target_file_size` | Default `max_compacted_files` |
| --- | --- | --- | --- |
| `micro` | `[0, 1 MiB)` | `5MB` | 64 |
| `small` | `[1 MiB, 10 MiB)` | `32MB` | 32 |
| `medium` | `[10 MiB, 64 MiB)` | `128MB` | 16 |

Files ≥ 64 MiB are left alone. A catalog `target_file_size` informs the merge target and is not overwritten when already set.

### Configuration precedence

**Built-in defaults → persisted lakemon policy → native DuckLake options → CALL named parameters.**

- Persisted lakemon policy (`CALL lakemon_set_policy`) holds the rewrite ladder and merge bands, stored in `__lakemon.policy` (not DuckLake `set_option` keys).
- Native DuckLake options (`rewrite_delete_threshold`, `target_file_size`, `expire_older_than`, `delete_older_than`, `auto_compact`) resolve table → schema → global, whether set via `set_option` or already present in the catalog.
- `lakemon_maintain` named parameters override table work for that `CALL`; `lakemon_maintain_global` named parameters override expire/cleanup intervals.

## API reference

```sql
CALL lakemon_table_stats('lake');
CALL lakemon_table_stats('lake', 'myschema.t');

CALL lakemon_maintain('lake');
CALL lakemon_maintain('lake', 'myschema.t');
CALL lakemon_maintain('lake', dry_run => true);

CALL lakemon_maintain_global('lake');
CALL lakemon_maintain_global('lake', expire_older_than => '7 days');
CALL lakemon_maintain_global('lake', dry_run => true);

-- Built-in ladder and merge bands
CALL lakemon_policy();
-- Effective policy for a catalog (defaults + persisted overrides)
CALL lakemon_policy('lake');
-- Persist ladder / band overrides (current DuckDB database, keyed by catalog)
CALL lakemon_set_policy('lake', 'rewrite_ladder', 'default', max_rewrite_steps => 4);
CALL lakemon_set_policy('lake', 'merge_tier', 'micro', target_file_size => '8MB');

SELECT lakemon_version();
```

### `CALL lakemon_maintain(catalog [, table])`

Table work only: inventory, flush, rewrite, merge. Does **not** expire snapshots or clean old/orphan files; retention parameters (`skip_expire`, `skip_cleanup`, `expire_older_than`, `delete_older_than`) belong on `lakemon_maintain_global`.

| Named parameter | Type | Default | Meaning |
| --- | --- | --- | --- |
| `dry_run` | `BOOLEAN` | `false` | Plan without running DuckLake SQL |
| `max_compacted_files` | `BIGINT` | tier default | Cap per merge-tier `CALL` |

`table` may be `table` (schema `main`) or `schema.table`. Catalog-wide `lakemon_maintain('lake')` loops every table and skips tables with `auto_compact = false`; an explicit table argument still maintains that table.

Result columns: `step`, `schema_name`, `table_name`, `action`, `status`, `files_processed`, `files_created`, `details`.

`dry_run => true` sets `status=planned` and appends the generated SQL in `details`. Execute sets `status=ok` for steps that ran. Rewrite emits **one result row per planned rung** (`action` is `rung_1` / `rung_2` / …, or `catalog` when DuckLake `rewrite_delete_threshold` is set). `details` always includes `band`, `delete_threshold`, `planned_files`, `planned_bytes`, and `planned_deletes`. `files_processed` is that rung's planned file count. `files_created` is the row count returned by that DuckLake `CALL` (typically one row per created file; `0` when the CALL returns no rows). `auto_compact` skips still list each computed rung.

Execute plans, then runs each non-skip DuckLake `CALL` as its own committed statement. A failed step keeps its planned SQL in `details`, appends the error, sets `status=error`, and marks later SQL steps `skip` with `not run: previous step failed`. The session stays usable. Interrupt cancels the query after marking remaining SQL steps; steps that already committed stay committed. Unread per-table metadata is an inventory `error` row (not a silent drop); tables that were read still proceed. A total inventory failure stops the plan. Out-of-memory is not swallowed.

### `CALL lakemon_maintain_global(catalog)`

Catalog-global retention only. Does **not** flush, rewrite, or merge tables.

| Named parameter | Type | Default | Meaning |
| --- | --- | --- | --- |
| `dry_run` | `BOOLEAN` | `false` | Plan without running DuckLake SQL |
| `skip_expire` | `BOOLEAN` | `false` | Skip snapshot expiration |
| `skip_cleanup` | `BOOLEAN` | `false` | Skip old-file and orphan cleanup |
| `expire_older_than` | `VARCHAR` | unset | Interval literal, e.g. `'7 days'` (skipped if unset) |
| `delete_older_than` | `VARCHAR` | unset | Interval literal for cleanup; otherwise `cleanup_all => true` |

When a named interval is omitted, lakemon uses `expire_older_than` / `delete_older_than` from `ducklake_options` / `catalog.options()` (usually global). Expire is skipped when no interval is set. Cleanup still runs with `cleanup_all => true` unless `skip_cleanup`. `older_than` is `now()::TIMESTAMP - INTERVAL …` so the expression binds without ICU.

Same result columns as `lakemon_maintain`; global steps leave `schema_name` / `table_name` empty. Default is plan and execute; `dry_run => true` returns the plan only.

### `CALL lakemon_table_stats(catalog [, table])`

One row per table: file counts and bytes, delete counts, **deleted_bytes_weighted**, delete ratio, **rewrite_plan** (each planned byte-weighted rung with derived threshold plus file/byte/delete counts), first-step `rewrite_threshold`, `merge_tier_hint`, `target_file_size`, and `auto_compact`. Stats apply the effective lakemon policy and native DuckLake options.

A missing catalog or unloaded `ducklake` raises a DuckDB error (session stays usable). Empty metadata returns no rows. Total unread metadata (no table could be read) also raises; the error includes every diagnostic. Maintain instead emits one inventory `error` row per diagnostic.

### `CALL lakemon_policy([catalog])`

Zero-argument form returns the built-in ladder and merge bands. With a catalog name, rows are the **effective** policy for that catalog (built-in defaults overlaid with rows in `__lakemon.policy`).

| Column | Meaning |
| --- | --- |
| `kind` | `rewrite_ladder` or `merge_tier` |
| `name` | `default` (rewrite ladder), or `micro` / `small` / `medium` |
| `min_value` / `max_value` | rewrite ladder rung range (`1` / `max_rewrite_steps`), or merge file-size band |
| `threshold_or_target` | `data-driven` (rewrite) or merge `target_file_size` |
| `notes` | `max_rewrite_steps` / `byte_budget` / `min_delete_ratio`, or `max_compacted_files` |
| `source` | `default` or `override` |

### `CALL lakemon_set_policy(catalog, kind, name, …)`

Writes the rewrite ladder or one merge band into `__lakemon.policy` in the **current DuckDB database** (creates schema `__lakemon` on first write). The catalog argument is a key only; the DuckLake catalog does not need to be attached. Rewrite name must be `default`. Merge names are `micro` / `small` / `medium`.

| Named parameter | Type | Applies to |
| --- | --- | --- |
| `max_rewrite_steps` | `BIGINT` | `rewrite_ladder` (ladder size `N`; `1`–`16`; default `3`) |
| `min_delete_ratio` | `DOUBLE` | `rewrite_ladder` (pool floor in `(0, 1]`; default `0.01`) |
| `byte_budget` | `BIGINT` | `rewrite_ladder` (`0` = unset; optional per-rung size cap) |
| `min_file_size` / `max_file_size` | `BIGINT` | `merge_tier` (`min < max`) |
| `target_file_size` | `VARCHAR` | `merge_tier` |
| `max_compacted_files` | `BIGINT` | `merge_tier` (`>= 1`) |
| `reset` | `BOOLEAN` | delete that override |
| `reset_all` | `BOOLEAN` | `CALL lakemon_set_policy(catalog, reset_all => true)` |

Omitted fields keep the current effective value (previous override or built-in). Result columns: `kind`, `name`, `status`, `details` (`ok` / `error`). Bind errors and `status=error` leave the DuckDB session usable.

### `lakemon_version()`

Scalar function returning the extension version string.

## Operating tips

- **Plan before you run.** `lakemon_table_stats` and `CALL lakemon_maintain` / `CALL lakemon_maintain_global` show the rungs (threshold, files, bytes), merge targets, and `auto_compact` skips before any file is touched.
- **Large backlog.** lakemon has no wall-clock budget today. Prefer one table at a time (`CALL lakemon_maintain('lake', 'schema.t')`) then `CALL lakemon_maintain_global('lake')`, a tighter policy via `lakemon_set_policy` / DuckLake `set_option`, or native `CALL`s with file caps (`max_compacted_files`). An unbounded full-catalog table pass can run a long time. Expire and orphan cleanup are catalog-global and do not need a per-table loop.
- **Single step by hand.** For one table step, call the native DuckLake functions directly (`ducklake_flush_inlined_data`, `ducklake_rewrite_data_files`, `ducklake_merge_adjacent_files`, `ducklake_expire_snapshots`, `ducklake_cleanup_old_files`, `ducklake_delete_orphaned_files`).
- **Zero rewrite threshold.** lakemon never plans or executes `ducklake_rewrite_data_files` with `delete_threshold` 0.0 (or <= 0). Use the native `CALL ducklake_rewrite_data_files(..., delete_threshold => 0)` if you need that.

Table maintain does not expire snapshots or clean old/orphan files; run table then global, for example `CALL lakemon_maintain('lake', 's.t'); CALL lakemon_maintain_global('lake');`.

## Building

This repository follows the [DuckDB C++ extension template](https://github.com/duckdb/extension-template). Clone with submodules:

```shell
git clone --recurse-submodules https://github.com/paulosuzart/lakemon-duckdb-ext.git
cd lakemon-duckdb-ext
```

Optional: `ccache` and `ninja` (`GEN=ninja make`) for faster rebuilds. No vcpkg packages are required.

```shell
make                 # builds DuckDB + the extension
make test            # SQL tests (extension smoke + lake execute when ducklake is built)
make policy-test     # header-only policy tests (no DuckDB)
make lake-test       # file-backed lake execute with a 60s hang timeout
make postgres-lake-test  # Postgres catalog execute (LAKEMON_CI_POSTGRES=1 build)
```

Binaries:

```
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/lakemon/lakemon.duckdb_extension
```

The in-tree shell already loads `lakemon`.

## Tests

- `make policy-test` — rewrite-ladder / merge-tier math, DuckLake option precedence, persisted policy overlays, and generated DuckLake CALL SQL (no DuckDB, seconds). Includes expire/cleanup `now()::TIMESTAMP` (no ICU).
- `make test` — SQL smoke: `lakemon_version`, `lakemon_policy` / `lakemon_set_policy`, bind errors / maintain plan rows when no catalog is attached, `lakemon_maintain_global` registration and retention skip/plan rows, ICU-free expire expression, and (when ducklake is built in) file-backed execute of flush+merge and expire
- `make lake-test` — `test/sql/lakemon_execute.test` under a 60s timeout so a hang fails
- `make postgres-lake-test` — same execute path against a Postgres DuckLake catalog (`LAKEMON_CI_POSTGRES=1` build, `LAKEMON_PG` connection string)
- CI: `make policy-test` and **Postgres lake execute** (DuckDB **1.5.5** and **2.x**, Postgres service, hang timeout, flush/merge/expire asserts) on every push/PR. Full DuckDB **1.5.5** and **2.x** (`v2.0-cyanoptera`) distribution builds run on `main`, `v*` tags, or **Actions → Main Extension Distribution Pipeline → Run workflow** (use that before a community listing submit). DuckDB **1.5.3** and **1.5.6** binaries are built only on `v*` tags or that same **Run workflow** path (not on ordinary pushes to `main`).

Manual check: `ATTACH` a lake, `CALL lakemon_set_policy(...)` for ladder size / merge bands, `CALL lake.set_option(...)` for the five honored DuckLake keys, then `CALL lakemon_table_stats` / `CALL lakemon_maintain(..., dry_run => true)` and confirm planned rewrite steps (rung + byte-derived threshold, never `0.0`), merge target, and `auto_compact` skips. Confirm expire/cleanup intervals with `CALL lakemon_maintain_global(..., dry_run => true)`. Execute with the same `CALL`s without `dry_run`.

## Community listing

See [community/README.md](community/README.md). A ready-to-copy `description.yml` lives in `community/description.yml`.

## License

MIT. See [LICENSE](LICENSE).
