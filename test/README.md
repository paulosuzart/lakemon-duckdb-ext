# Tests

## Policy (no DuckDB)

```shell
make policy-test
```

Compiles `test/policy/test_policy.cpp`, `test_options.cpp`, `test_overrides.cpp`, `test_sql.cpp`, and `test_observe.cpp` against the header-only policy/options/diagnostics/SQL/progress helpers (no DuckDB). Includes the flush-inlined delete-file limit helpers.

## Extension smoke

```shell
make test
```

SQL logic tests under `test/sql/`:

- `lakemon.test` — version + `lakemon_policy` rewrite ladder / merge tiers + `lakemon_set_policy` persist/read
- `lakemon_maintain.test` — bind errors when no DuckLake catalog is attached; `lakemon_maintain_global` skip/plan rows; retention named params rejected on table maintain
- `lakemon_expire_no_icu.test` — `now()::TIMESTAMP - INTERVAL` binds without loading ICU
- `lakemon_execute.test` — file-backed DuckLake catalog: `CALL` / `SELECT` execute of flush+merge and expire (`dry_run => false`). `require ducklake`. `make lake-test` wraps this file in a 60s timeout.

`make test` runs every `.test` file. `lakemon_execute.test` is skipped when ducklake is not in the unittest binary. Ducklake is built only when `LAKEMON_CI_POSTGRES=1` (Postgres lake CI). Default and distribution builds omit it so they do not need roaring from vcpkg.

## Postgres catalog execute

```shell
# build with postgres_scanner (needs libpq)
LAKEMON_CI_POSTGRES=1 make
# Postgres reachable as $LAKEMON_PG (default: local lakemon/postgres/postgres)
make postgres-lake-test
```

`test/run_postgres_lake.py` attaches `ducklake:postgres:…`, seeds inlined rows + many small files + deletes, then `CALL lakemon_maintain(..., dry_run => false)` and `CALL lakemon_maintain_global(..., expire_older_than => '0 days')`. A hang timeout treats a stuck `CALL` as failure. It asserts inlined rows were flushed, data files decreased after merge, snapshot count dropped after expire, stderr contains per-step start/end/`progress=` lines (`lakemon_log_stderr`), and no step duration exceeds a generous cap. `duckdb_logs()` is not asserted (storage / log-type differ on 1.5.5 vs 2.x). A dedicated table seeds inlined deletes on parquet files, rewrites those files (ended), and asserts `lakemon_table_stats` plus a skipped `flush_inlined` when `lakemon_flush_inlined_max_delete_files` is below the planned delete-file count (rewrite/merge still run). A second pass seeds hundreds of small files and dumps `pg_locks` / `pg_stat_activity` during maintain. A concurrent pass holds a second DuckDB session (observational) and a Postgres `EXCLUSIVE` lock on the catalog snapshot relation. Maintain is ATTACH'd with `options=-clock_timeout=5s` so DuckLake's commit connection fails fast; it must return `status=error` with lock/timeout details (not hang). The test does not `ALTER DATABASE` `lock_timeout`.

GitHub Actions workflow **Postgres lake execute** runs this on every pull request for DuckDB 1.5.5, 1.5.6, and 2.x. It also checks that `EXPLAIN`, `PREPARE`, `CREATE VIEW`, `BEGIN`, and a parameterized `?` bind with `dry_run => false` error without changing snapshot or file counts, and that one `CALL` adds one snapshot per ok mutating step.
