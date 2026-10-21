# Updating the DuckDB target

This repository follows the C++ [extension-template](https://github.com/duckdb/extension-template) update path.

1. Bump the `duckdb` submodule to the tagged release you want (for example `v1.5.5`). Keep this on the everyday 1.5.x pin: CodeQuality and local `make` use it. Community CI does **not** use this submodule; it checks out DuckDB itself (`v2.0.0` for `repo.ref`).
2. Bump `extension-ci-tools` to the matching **branch** (`v1.5.5`, `v1.5.6`, or `v2.0-cyanoptera` for 2.0). There is no `v2.0.0` tag on `extension-ci-tools`.
3. Update `.github/workflows/MainDistributionPipeline.yml`:
   - `duckdb-stable-build` → latest everyday 1.5.x pin (currently `v1.5.5`)
   - `duckdb-1-5-3-build` / `duckdb-1-5-6-build` → matching `duckdb_version` + `ci_tools_version` (release/dispatch only)
   - `duckdb-2-build` → `duckdb_version: v2.0.0` and `ci_tools_version: v2.0-cyanoptera`
4. Rebuild and run `make test` plus `make policy-test`.
5. Before merge or a community listing submit, run **Actions → Main Extension Distribution Pipeline → Run workflow** on the branch (that workflow does not run on ordinary feature-branch pushes). Dispatch also builds the extra 1.5.3 and 1.5.6 matrices. A pull request that changes `extension_config.cmake`, `vcpkg.json`, or the distribution workflow itself also runs the everyday 1.5.5 + 2.0.0 matrices.

## Distribution matrix

DuckDB loads an extension only when it was compiled for that exact DuckDB version. The pipeline therefore produces separate binaries:

| DuckDB version | ci-tools pin | Built on |
| --- | --- | --- |
| `v1.5.5` | `v1.5.5` | push to `main`, `v*` tags, `workflow_dispatch` |
| `v2.0.0` | `v2.0-cyanoptera` | push to `main`, `v*` tags, `workflow_dispatch` |
| `v1.5.3` | `v1.5.3` | `v*` tags and `workflow_dispatch` only |
| `v1.5.6` | `v1.5.6` | `v*` tags and `workflow_dispatch` only |

Each job uploads GitHub Actions artifacts named `lakemon-<duckdb_version>-extension-<arch>`.

## Community `description.yml` refs

`duckdb/community-extensions` `scripts/build.py` (with `DUCKDB_LATEST_STABLE=v2.0.0`):

- `repo.ref` — source SHA for latest stable DuckDB (**v2.0.0**).
- `repo.variegata` — optional SHA for DuckDB **v1.5.6** rebuilds; omitted means `ref`.
- `repo.andium` — optional SHA for DuckDB **v1.4.5**.
- `repo.ref_next` — optional SHA when `DUCKDB_VERSION` is not latest stable (upcoming line; `build_next.yml` is currently disabled).
- There is no `ref_stable` field.

Same lakemon source compiles for 1.5.x and 2.0.0 (`lakemon_compat.hpp`), so one `ref` SHA is enough.

## Cutting a GitHub Release

Do this after the version/build change is on `main`. This repo does not create the tag or the Release from CI.

1. Pick a tag that matches `v*` (for example `v0.1.1`) on the commit you want to ship.
2. Push the tag: `git tag v0.1.1 && git push origin v0.1.1`. That starts **Main Extension Distribution Pipeline** and builds **1.5.3, 1.5.5, 1.5.6, and 2.0.0**.
   Alternatively, **Actions → Main Extension Distribution Pipeline → Run workflow** on that commit (same four versions; artifacts stay on the Actions run until you attach them to a Release).
3. When the run is green, open it and download the artifacts (`lakemon-v1.5.3-extension-*`, `lakemon-v1.5.5-extension-*`, `lakemon-v1.5.6-extension-*`, `lakemon-v2.0.0-extension-*`).
4. **Releases → Draft a new release**, choose the same `v*` tag, attach the downloaded binaries (the `.duckdb_extension` files, or the artifact zips), and publish.
5. Users load a binary that matches their DuckDB version with `duckdb -unsigned` and `LOAD '/path/to/lakemon.duckdb_extension'`.

The C++ extension API is **not** a stable ABI. A DuckDB bump may require source changes. Useful references:

- DuckDB [release notes](https://github.com/duckdb/duckdb/releases)
- [community-extensions UPDATING.md](https://github.com/duckdb/community-extensions/blob/main/UPDATING.md) (`ref` vs `ref_next`)
