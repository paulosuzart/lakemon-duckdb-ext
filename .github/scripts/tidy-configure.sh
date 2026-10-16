#!/usr/bin/env bash
# CMake-configure DuckDB + this extension with CMAKE_EXPORT_COMPILE_COMMANDS.
# No compile/link of DuckDB. Writes build/tidy/compile_commands.json and
# asserts lakemon translation units are present.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if [[ ! -f duckdb/CMakeLists.txt ]]; then
	echo "::error::duckdb submodule is missing; init duckdb before tidy-configure"
	exit 1
fi
if [[ ! -f duckdb/.clang-tidy ]]; then
	echo "::error::duckdb/.clang-tidy is missing on the pinned DuckDB revision"
	exit 1
fi
if [[ ! -f .clang-tidy ]]; then
	echo "::error::.clang-tidy is missing (expected symlink to duckdb/.clang-tidy)"
	exit 1
fi

mkdir -p build/tidy

# OVERRIDE_GIT_DESCRIBE avoids a shallow-clone git-describe failure.
# CLANG_TIDY=1 skips tests/python bits; DISABLE_UNITY=1 so each .cpp is a
# compile_commands entry (needed to run clang-tidy on src/*.cpp).
# BUILD_SHELL/UNITTESTS off: configure-only, no DuckDB compile.
# CMAKE_CXX_STANDARD=17 matches src CMakeLists.txt (DuckDB's CACHE default is 11).
cmake -S duckdb -B build/tidy \
	-DCMAKE_BUILD_TYPE=Debug \
	-DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
	-DCMAKE_CXX_STANDARD=17 \
	-DCLANG_TIDY=1 \
	-DDISABLE_UNITY=1 \
	-DBUILD_SHELL=0 \
	-DBUILD_UNITTESTS=0 \
	-DEXTENSION_STATIC_BUILD=1 \
	-DENABLE_UNITTEST_CPP_TESTS=FALSE \
	-DENABLE_EXTENSION_AUTOLOADING=0 \
	-DENABLE_EXTENSION_AUTOINSTALL=0 \
	-DDUCKDB_EXTENSION_CONFIGS="${ROOT}/extension_config.cmake" \
	-DOVERRIDE_GIT_DESCRIBE=v1.5.5

db="build/tidy/compile_commands.json"
if [[ ! -f "$db" ]]; then
	echo "::error::cmake did not write ${db}"
	exit 1
fi

python3 - "$ROOT" "$db" <<'PY'
import json, os, sys

root = os.path.abspath(sys.argv[1])
src_root = os.path.join(root, "src") + os.sep
db_path = sys.argv[2]
entries = json.load(open(db_path))
ours = []
seen = set()
for entry in entries:
    path = entry.get("file") or ""
    if not os.path.isabs(path):
        path = os.path.normpath(os.path.join(entry.get("directory", "."), path))
    path = os.path.abspath(path)
    if path.startswith(src_root) and path.endswith((".c", ".cc", ".cpp")) and path not in seen:
        seen.add(path)
        ours.append(path)

print(f"compile_commands.json entries: {len(entries)}")
print(f"lakemon translation units: {len(ours)}")
for path in sorted(ours):
    print(f"  {os.path.relpath(path, root)}")

if not ours:
    sample = []
    for entry in entries[:20]:
        sample.append(entry.get("file", ""))
    print("sample compile_commands files:")
    for item in sample:
        print(f"  {item}")
    sys.exit("cmake configure succeeded but no src/*.cpp translation units are in compile_commands.json")
PY
