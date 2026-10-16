# Updating the DuckDB target

This repository follows the C++ [extension-template](https://github.com/duckdb/extension-template) update path.

1. Bump the `duckdb` submodule to the tagged release you want (for example `v1.5.5`).
2. Bump `extension-ci-tools` to the matching branch (`v1.5.5`, `v1.5-variegata`, or `main` for 2.x). `extension-ci-tools` uses version **branches**, not git tags, for 1.5.x pins.
3. Update `.github/workflows/MainDistributionPipeline.yml`:
   - `duckdb-stable-build` → latest everyday 1.5.x pin (currently `v1.5.5`)
   - `duckdb-1-5-3-build` / `duckdb-1-5-6-build` → matching `duckdb_version` + `ci_tools_version` (release/dispatch only)
   - `duckdb-2-build` → `v2.0-cyanoptera` / `main` until 2.0.0 is tagged
4. Rebuild and run `make test` plus `make policy-test`.
5. Before merge or a community listing submit, run **Actions → Main Extension Distribution Pipeline → Run workflow** on the branch (that workflow does not run on ordinary feature-branch pushes). Dispatch also builds the extra 1.5.3 and 1.5.6 matrices. A pull request that changes `extension_config.cmake`, `vcpkg.json`, or the distribution workflow itself also runs the everyday 1.5.5 + 2.x matrices.

## Distribution matrix

DuckDB loads an extension only when it was compiled for that exact DuckDB version. The pipeline therefore produces separate binaries:

| DuckDB version | ci-tools pin | Built on |
| --- | --- | --- |
| `v1.5.5` | `v1.5.5` | push to `main`, `v*` tags, `workflow_dispatch` |
| `v2.0-cyanoptera` | `main` | push to `main`, `v*` tags, `workflow_dispatch` |
| `v1.5.3` | `v1.5.3` | `v*` tags and `workflow_dispatch` only |
| `v1.5.6` | `v1.5.6` | `v*` tags and `workflow_dispatch` only |

Each job uploads GitHub Actions artifacts named `lakemon-<duckdb_version>-extension-<arch>`.

## Cutting a GitHub Release

Do this after the version/build change is on `main`. This repo does not create the tag or the Release from CI.

1. Pick a tag that matches `v*` (for example `v0.1.0`) on the commit you want to ship.
2. Push the tag: `git tag v0.1.0 && git push origin v0.1.0`. That starts **Main Extension Distribution Pipeline** and builds **1.5.3, 1.5.5, 1.5.6, and 2.x**.
   Alternatively, **Actions → Main Extension Distribution Pipeline → Run workflow** on that commit (same four versions; artifacts stay on the Actions run until you attach them to a Release).
3. When the run is green, open it and download the artifacts (`lakemon-v1.5.3-extension-*`, `lakemon-v1.5.5-extension-*`, `lakemon-v1.5.6-extension-*`, `lakemon-v2.0-cyanoptera-extension-*`).
4. **Releases → Draft a new release**, choose the same `v*` tag, attach the downloaded binaries (the `.duckdb_extension` files, or the artifact zips), and publish.
5. Users load a binary that matches their DuckDB version with `duckdb -unsigned` and `LOAD '/path/to/lakemon.duckdb_extension'`.

The C++ extension API is **not** a stable ABI. A DuckDB bump may require source changes. Useful references:

- DuckDB [release notes](https://github.com/duckdb/duckdb/releases)
- [community-extensions UPDATING.md](https://github.com/duckdb/community-extensions/blob/main/UPDATING.md) (`ref` vs `ref_next`)
