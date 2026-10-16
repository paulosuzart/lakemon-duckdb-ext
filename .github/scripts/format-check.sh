#!/usr/bin/env bash
# Check src/ (including src/include) against DuckDB's .clang-format.
# Prints unified diffs, GitHub annotations, and a step summary.
# Exit 1 when any file is out of format.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if [[ ! -f .clang-format ]]; then
	echo "::error::.clang-format is missing (expected symlink to duckdb/.clang-format)"
	exit 1
fi
if [[ ! -f duckdb/.clang-format ]]; then
	echo "::error::duckdb/.clang-format is missing; init the duckdb submodule"
	exit 1
fi

if ! command -v clang-format >/dev/null 2>&1; then
	echo "::error::clang-format not on PATH"
	exit 1
fi

version="$(clang-format --version)"
echo "clang-format: ${version}"
if [[ "${version}" != *11.* ]]; then
	echo "::error::DuckDB requires clang-format 11.x (pip install clang_format==11.0.1); got: ${version}"
	exit 1
fi

mapfile -t files < <(
	find src -type f \( \
		-name '*.c' -o -name '*.cc' -o -name '*.cpp' -o \
		-name '*.h' -o -name '*.hh' -o -name '*.hpp' -o -name '*.ipp' \
	\) | LC_ALL=C sort
)

if [[ ${#files[@]} -eq 0 ]]; then
	echo "::error::no C/C++ files under src/"
	exit 1
fi

echo "Checking ${#files[@]} files under src/ with --sort-includes=0 -style=file"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

bad=()
annotation_budget=40
: >"${tmp}/diffs.txt"

for f in "${files[@]}"; do
	formatted="${tmp}/formatted"
	if ! clang-format --sort-includes=0 -style=file "$f" >"$formatted"; then
		echo "::error file=${f}::clang-format failed to parse this file"
		bad+=("$f")
		continue
	fi
	if ! diff -q "$f" "$formatted" >/dev/null; then
		bad+=("$f")
		{
			echo
			diff -u "$f" "$formatted" || true
		} >>"${tmp}/diffs.txt"
		if ((annotation_budget > 0)); then
			echo "::warning file=${f},line=1::not clang-formatted (clang-format 11.0.1 / DuckDB style)"
			annotation_budget=$((annotation_budget - 1))
		fi
	fi
done

{
	echo "## Format check (advisory)"
	echo
	echo "- clang-format: \`${version}\`"
	echo "- style: repo-root \`.clang-format\` → \`duckdb/.clang-format\`"
	echo "- files checked: ${#files[@]}"
	echo "- files out of format: ${#bad[@]}"
	echo
	if [[ ${#bad[@]} -eq 0 ]]; then
		echo "All files under \`src/\` match DuckDB clang-format."
	else
		echo "### Files out of format"
		echo
		for f in "${bad[@]}"; do
			echo "- \`${f}\`"
		done
		echo
		echo "Reformat is intentionally not applied in this PR. Unified diffs are in the job log."
	fi
} | tee "${tmp}/summary.md"

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
	cat "${tmp}/summary.md" >>"$GITHUB_STEP_SUMMARY"
fi

if [[ ${#bad[@]} -gt 0 ]]; then
	echo
	echo "===== clang-format diffs ====="
	cat "${tmp}/diffs.txt"
	echo
	echo "FORMAT_FILES_OUT_OF_STYLE=${#bad[@]}"
	exit 1
fi

echo "FORMAT_FILES_OUT_OF_STYLE=0"
exit 0
