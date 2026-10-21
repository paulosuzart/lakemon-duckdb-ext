# Updating the DuckDB target

Starting with lakemon **0.2.0**, only DuckDB **1.5.6** and **2.0.0** are supported. Older DuckDB 1.5.x binaries (1.5.3, 1.5.5, …) are no longer produced.

This repository follows the C++ [extension-template](https://github.com/duckdb/extension-template) update path.

1. Bump the `duckdb` submodule to the everyday 1.5.x pin (currently `v1.5.6`). CodeQuality and local `make` use it. Community CI does **not** use this submodule; it checks out DuckDB itself (`v2.0.0` for `repo.ref`).
2. Bump `extension-ci-tools` to the matching **branch** (`v1.5.6`, or `v2.0-cyanoptera` for 2.0). There is no `v2.0.0` tag on `extension-ci-tools`.
3. Update `.github/workflows/MainDistributionPipeline.yml`:
   - `duckdb-stable-build` → `v1.5.6`
   - `duckdb-2-build` → `duckdb_version: v2.0.0` and `ci_tools_version: v2.0-cyanoptera`
4. Rebuild and run `make test` plus `make policy-test`.
5. Before merge or a community listing submit, run **Actions → Main Extension Distribution Pipeline → Run workflow** on the branch (that workflow does not run on ordinary feature-branch pushes). A pull request that changes `extension_config.cmake`, `vcpkg.json`, or the distribution workflow itself also runs the 1.5.6 + 2.0.0 matrices.

## Distribution matrix

DuckDB loads an extension only when it was compiled for that exact DuckDB version. The pipeline therefore produces separate binaries:

| DuckDB version | ci-tools pin | Built on |
| --- | --- | --- |
| `v1.5.6` | `v1.5.6` | push to `main`, `v*` tags, `workflow_dispatch` |
| `v2.0.0` | `v2.0-cyanoptera` | push to `main`, `v*` tags, `workflow_dispatch` |

Each job uploads GitHub Actions artifacts named `lakemon-<duckdb_version>-extension-<arch>`.

## Community `description.yml` refs

Public docs ([Submit a Community Extension](https://duckdb.org/community_extensions/documentation)) list only `repo.ref`. Extra keys live in [`scripts/build.py`](https://github.com/duckdb/community-extensions/blob/main/scripts/build.py) (read with `DUCKDB_LATEST_STABLE` / `DUCKDB_VERSION` from [`.github/workflows/build.yml`](https://github.com/duckdb/community-extensions/blob/main/.github/workflows/build.yml), currently `v2.0.0`). There is no `ref_stable`.

| Field | Used for |
| --- | --- |
| `repo.ref` | Latest stable DuckDB (**v2.0.0**). Default community build. |
| `repo.variegata` | Optional SHA when `DUCKDB_VERSION == 'v1.5.6'`. Omitted → `ref`. |
| `repo.andium` | Optional SHA for DuckDB **v1.4.5**. |
| `repo.ref_next` | Optional SHA when `DUCKDB_VERSION` is **not** latest stable (upcoming line; `build_next.yml` is currently disabled). |

Same lakemon source compiles for 1.5.6 and 2.0.0 (`lakemon_compat.hpp`), so one `ref` SHA is enough; leave `variegata` unset unless a 1.5.6-only commit exists.

## Cutting a GitHub Release

Do this after the version/build change is on `main`. This repo does not create the tag or the Release from CI.

1. Pick a tag that matches `v*` (for example `v0.2.0`) on the commit you want to ship.
2. Push the tag: `git tag v0.2.0 && git push origin v0.2.0`. That starts **Main Extension Distribution Pipeline** and builds **1.5.6 and 2.0.0**.
   Alternatively, **Actions → Main Extension Distribution Pipeline → Run workflow** on that commit (same two versions; artifacts stay on the Actions run until you attach them to a Release).
3. When the run is green, open it and download the artifacts (`lakemon-v1.5.6-extension-*`, `lakemon-v2.0.0-extension-*`).
4. **Releases → Draft a new release**, choose the same `v*` tag, attach the downloaded binaries (the `.duckdb_extension` files, or the artifact zips), and publish.
5. Users load a binary that matches their DuckDB version with `duckdb -unsigned` and `LOAD '/path/to/lakemon.duckdb_extension'`.

The C++ extension API is **not** a stable ABI. A DuckDB bump may require source changes. Useful references:

- DuckDB [release notes](https://github.com/duckdb/duckdb/releases)
- [community-extensions UPDATING.md](https://github.com/duckdb/community-extensions/blob/main/UPDATING.md) (`ref` vs `ref_next`)
