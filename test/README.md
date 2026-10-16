# Tests

## Policy (no DuckDB)

```shell
make policy-test
```

Compiles `test/policy/test_policy.cpp`, `test_options.cpp`, `test_overrides.cpp`, and `test_sql.cpp` against the header-only policy/options/diagnostics/SQL helpers (no DuckDB).

## Extension smoke

```shell
make test
```

SQL logic tests under `test/sql/`:

- `lakemon.test` — version + `lakemon_policy` rewrite ladder / merge tiers + `lakemon_set_policy` persist/read
- `lakemon_maintain.test` — bind errors when no DuckLake catalog is attached; `lakemon_maintain_global` skip/plan rows; retention named params rejected on table maintain
- `lakemon_expire_no_icu.test` — `now()::TIMESTAMP - INTERVAL` binds without loading ICU
- `lakemon_execute.test` — file-backed DuckLake catalog: `CALL` / `SELECT` execute of flush+merge and expire (`dry_run => false`). `require ducklake`. `make lake-test` wraps this file in a 60s timeout.

`make test` runs every `.test` file. `lakemon_execute.test` is skipped when ducklake is not in the unittest binary.

## Postgres catalog execute

```shell
# build with postgres_scanner (needs libpq)
LAKEMON_CI_POSTGRES=1 make
# Postgres reachable as $LAKEMON_PG (default: local lakemon/postgres/postgres)
make postgres-lake-test
```

`test/run_postgres_lake.py` attaches `ducklake:postgres:…`, seeds inlined rows + many small files + deletes, then `CALL lakemon_maintain(..., dry_run => false)` and `CALL lakemon_maintain_global(..., expire_older_than => '0 days')`. A 90s timeout treats a hang as failure. It asserts inlined rows were flushed, data files decreased after merge, and snapshot count dropped after expire.

GitHub Actions workflow **Postgres lake execute** runs this on every pull request for DuckDB 1.5.5 and 2.x.
