#!/usr/bin/env python3
"""Run lakemon_execute.test with a timeout so a hang fails the test."""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
UNITTEST = ROOT / "build" / "release" / "test" / "unittest"
TIMEOUT_SECS = int(sys.argv[1]) if len(sys.argv) > 1 else 60


def main() -> int:
	if not UNITTEST.is_file():
		print("build/release/test/unittest is missing; run make first", file=sys.stderr)
		return 1
	try:
		result = subprocess.run(
			[str(UNITTEST), "test/sql/lakemon_execute.test"],
			cwd=str(ROOT),
			timeout=TIMEOUT_SECS,
		)
	except subprocess.TimeoutExpired:
		print(
			f"TIMEOUT: test/sql/lakemon_execute.test exceeded {TIMEOUT_SECS}s (hang)",
			file=sys.stderr,
		)
		return 1
	return result.returncode


if __name__ == "__main__":
	sys.exit(main())
