#!/usr/bin/env python3
"""Parse clang-tidy output, emit GitHub annotations + step summary, exit 1 on findings."""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections import Counter, defaultdict

DIAG_RE = re.compile(
    r"^(?P<file>.+):(?P<line>\d+):(?P<col>\d+): "
    r"(?P<sev>warning|error): (?P<msg>.*?)(?: \[(?P<check>[^\]]+)\])?$"
)


def is_under_src(path: str, src_root: str) -> bool:
    path = os.path.abspath(path)
    return path == src_root or path.startswith(src_root + os.sep)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--log", required=True)
    parser.add_argument("--tidy-rc", type=int, default=0)
    args = parser.parse_args()

    root = os.path.abspath(args.root)
    src_root = os.path.join(root, "src")
    text = open(args.log, encoding="utf-8", errors="replace").read()
    findings = []
    for raw in text.splitlines():
        match = DIAG_RE.match(raw)
        if not match:
            continue
        path = match.group("file")
        if not is_under_src(path, src_root):
            continue
        rel = os.path.relpath(path, root)
        check = match.group("check") or "clang-diagnostic"
        check = check.split(",")[0].strip()
        findings.append(
            {
                "file": rel,
                "line": int(match.group("line")),
                "col": int(match.group("col")),
                "sev": match.group("sev"),
                "msg": match.group("msg"),
                "check": check,
            }
        )

    unique = []
    seen = set()
    for item in findings:
        key = (item["file"], item["line"], item["col"], item["check"])
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    findings = unique

    by_check: Counter[str] = Counter(item["check"] for item in findings)
    by_file: dict[str, list] = defaultdict(list)
    for item in findings:
        by_file[item["file"]].append(item)

    lines = [
        "## Tidy check (advisory)",
        "",
        "- compile_commands: `build/tidy/compile_commands.json` (CMake configure only; no DuckDB compile)",
        "- config: repo-root `.clang-tidy` → `duckdb/.clang-tidy`",
        f"- findings in `src/`: **{len(findings)}**",
        f"- clang-tidy exit code: {args.tidy_rc}",
        "",
    ]
    if findings:
        lines += ["### Findings by check", "", "| Check | Count |", "| --- | ---: |"]
        for check, count in sorted(by_check.items(), key=lambda kv: (-kv[1], kv[0])):
            lines.append(f"| `{check}` | {count} |")
        lines += ["", "### Findings by file", ""]
        for file, items in sorted(by_file.items()):
            lines.append(f"- `{file}`: {len(items)}")
        lines += [
            "",
            "Tidy is advisory until findings are cleaned by category. Full diagnostics are in the job log.",
        ]
    else:
        lines.append("No clang-tidy diagnostics in `src/` (including `src/include`).")

    summary = "\n".join(lines) + "\n"
    sys.stdout.write(summary)
    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a", encoding="utf-8") as handle:
            handle.write(summary)

    budget = 50
    for item in findings:
        if budget <= 0:
            break
        check = item["check"]
        print(
            f"::{item['sev']} file={item['file']},line={item['line']},col={item['col']}::[{check}] {item['msg']}"
        )
        budget -= 1

    print("TIDY_FINDINGS={}".format(len(findings)))
    for check, count in sorted(by_check.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"TIDY_CHECK {count}\t{check}")

    if findings or args.tidy_rc != 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
