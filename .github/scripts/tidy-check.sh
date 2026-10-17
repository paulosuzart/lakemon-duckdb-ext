#!/usr/bin/env bash
# Run clang-tidy on lakemon translation units from build/tidy/compile_commands.json.
# Uses the repo-root .clang-tidy symlink (DuckDB config) and overrides
# HeaderFilterRegex so findings in src/ (including headers) are reported.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

db="build/tidy/compile_commands.json"
if [[ ! -f "$db" ]]; then
	echo "::error::${db} missing; run tidy-configure first"
	exit 1
fi
if ! command -v clang-tidy >/dev/null 2>&1; then
	echo "::error::clang-tidy not on PATH"
	exit 1
fi

echo "clang-tidy: $(clang-tidy --version | head -n 1)"
echo "config: $(readlink -f .clang-tidy)"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

mapfile -t tus < <(python3 - "$ROOT" "$db" <<'PY'
import json, os, sys
root = os.path.abspath(sys.argv[1])
src_root = os.path.join(root, "src") + os.sep
entries = json.load(open(sys.argv[2]))
seen = set()
for entry in entries:
    path = entry.get("file") or ""
    if not os.path.isabs(path):
        path = os.path.normpath(os.path.join(entry.get("directory", "."), path))
    path = os.path.abspath(path)
    if path.startswith(src_root) and path.endswith((".c", ".cc", ".cpp")) and path not in seen:
        seen.add(path)
        print(path)
PY
)

if [[ ${#tus[@]} -eq 0 ]]; then
	echo "::error::no lakemon translation units in compile_commands.json"
	exit 1
fi

header_filter="${ROOT}/src/"
nproc_jobs="$(nproc 2>/dev/null || echo 2)"
if [[ "$nproc_jobs" -gt 2 ]]; then
	jobs=2
else
	jobs="$nproc_jobs"
fi

# compile_commands.json is produced by g++; clang-tidy parses with clang
# and may not see the same libstdc++ search path. Inject g++ system includes.
extra_args=()
mapfile -t gcc_isystem < <(
	echo | "${CXX:-g++}" -xc++ -E -v - 2>&1 \
		| awk '/^#include <...> search starts here:/{f=1; next} /^End of search list\./{f=0} f{gsub(/^[ \t]+/,""); print}'
)
echo "g++ system includes for clang-tidy:"
for dir in "${gcc_isystem[@]}"; do
	echo "  ${dir}"
	extra_args+=("--extra-arg=-isystem${dir}")
done
extra_args+=("--extra-arg=-Wno-unknown-warning-option")
extra_args+=("--extra-arg=-Wno-unused-command-line-argument")

echo "Running clang-tidy on ${#tus[@]} files (-j ${jobs})"
echo "header-filter: ${header_filter}"

set +e
printf '%s\0' "${tus[@]}" | xargs -0 -P "$jobs" -I{} \
	clang-tidy -quiet -p build/tidy -header-filter="$header_filter" "${extra_args[@]}" {} \
	> "${tmp}/tidy.log" 2>&1
tidy_rc=$?
set -e
cat "${tmp}/tidy.log"

python3 "${ROOT}/.github/scripts/report_clang_tidy.py" \
	--root "$ROOT" \
	--log "${tmp}/tidy.log" \
	--tidy-rc "$tidy_rc"
report_rc=$?

if [[ "$report_rc" -ne 0 ]]; then
	exit "$report_rc"
fi
if [[ "$tidy_rc" -ne 0 ]]; then
	exit "$tidy_rc"
fi
exit 0
