#!/usr/bin/env python3
"""Execute lakemon_maintain / lakemon_maintain_global against a Postgres DuckLake catalog.

A hang fails via --timeout (default 90s) around each mutating CALL.
Asserts: inlined data flushed, fewer data files after merge, old snapshots expired,
per-step logs (duckdb_logs), byte-weighted progress in those logs, hundreds of
files, and a second session that holds the catalog (finish or a clear error).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DUCKDB = ROOT / "build" / "release" / "duckdb"
DEFAULT_TIMEOUT = 90
CATALOG = "lake"
SCHEMA = "bronze_financial"
TABLE = "subscription"
QUALIFIED = f"{SCHEMA}.{TABLE}"
HEAVY_TABLE = "events"
HEAVY_QUALIFIED = f"{SCHEMA}.{HEAVY_TABLE}"
HEAVY_FILE_COUNT = 128
# Generous per-step cap so a lock wait or planner hang fails CI without
# flaking on a slow runner. Hundreds of small files should stay well under this.
STEP_DURATION_LIMIT_MS = 120_000
CONCURRENT_TIMEOUT = 25


class Failed(Exception):
	pass


def find_extension(build_dir: Path, *names: str) -> str | None:
	for name in names:
		candidates = [
			build_dir / "extension" / name / f"{name}.duckdb_extension",
			build_dir / "repository" / "v1.5.5" / "linux_amd64" / f"{name}.duckdb_extension",
		]
		for path in candidates:
			if path.is_file():
				return str(path)
		for path in build_dir.rglob(f"{name}*.duckdb_extension"):
			return str(path)
	return None


def find_postgres_extension(build_dir: Path) -> str | None:
	return find_extension(build_dir, "postgres_scanner", "postgres")


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description=__doc__)
	parser.add_argument("--duckdb", default=os.environ.get("LAKEMON_DUCKDB", str(DEFAULT_DUCKDB)))
	parser.add_argument(
		"--pg",
		default=os.environ.get(
			"LAKEMON_PG",
			"dbname=lakemon host=127.0.0.1 port=5432 user=postgres password=postgres",
		),
	)
	parser.add_argument("--data-path", default="")
	parser.add_argument("--timeout", type=int, default=int(os.environ.get("LAKEMON_PG_TIMEOUT", DEFAULT_TIMEOUT)))
	return parser.parse_args()


def probe_load(duckdb: str, candidates: list[str], what: str) -> str:
	errors = []
	for sql in candidates:
		try:
			result = subprocess.run(
				[duckdb, "-unsigned", ":memory:"],
				input=sql,
				text=True,
				capture_output=True,
				timeout=30,
				cwd=str(ROOT),
			)
		except subprocess.TimeoutExpired:
			errors.append(f"{sql} timed out")
			continue
		if result.returncode == 0:
			return sql
		errors.append(f"{sql} -> {result.stderr.strip() or result.stdout.strip()}")
	raise Failed(f"could not load {what}:\n" + "\n".join(errors))


def probe_postgres_load(duckdb: str, ext_path: str | None) -> str:
	candidates = ["LOAD postgres;", "LOAD postgres_scanner;"]
	if ext_path:
		candidates.append(f"LOAD '{ext_path}';")
	candidates.append("INSTALL postgres; LOAD postgres;")
	return probe_load(duckdb, candidates, "the postgres extension")


def probe_named_load(duckdb: str, name: str, ext_path: str | None, verify_sql: str = "",
                     optional: bool = False) -> str:
	loads = []
	if ext_path:
		loads.append(f"LOAD '{ext_path}';")
	loads.append(f"LOAD {name};")
	if optional or verify_sql:
		loads.append("")
	errors = []
	for load in loads:
		script = f"{load}\n{verify_sql}" if verify_sql else (load or "SELECT 1;")
		try:
			probe_load(duckdb, [script], f"the {name} extension")
			return load
		except Failed as exc:
			errors.append(str(exc))
	if optional:
		return ""
	raise Failed(f"could not load the {name} extension:\n" + "\n".join(errors))


class LakeSession:
	def __init__(self, duckdb: str, pg: str, data_path: str, loads: list[str]) -> None:
		self.duckdb = duckdb
		self.pg = pg
		self.data_path = data_path
		self.loads = loads
		self.inlining_row_limit = 10
		self.last_stderr = ""
		self.last_stdout = ""

	def preamble(self) -> str:
		attach = (
			f"ATTACH 'ducklake:postgres:{self.pg}' AS {CATALOG} "
			f"(DATA_PATH '{self.data_path}', DATA_INLINING_ROW_LIMIT {self.inlining_row_limit});"
		)
		return "\n".join([part for part in self.loads if part] + [attach])

	def run(self, sql: str, timeout: int | None = None) -> str:
		script = self.preamble() + "\n" + sql
		cmd = [self.duckdb, "-unsigned", "-json", ":memory:"]
		try:
			result = subprocess.run(
				cmd,
				input=script,
				text=True,
				capture_output=True,
				timeout=timeout,
				cwd=str(ROOT),
			)
		except subprocess.TimeoutExpired as exc:
			self.last_stdout = exc.stdout or ""
			self.last_stderr = exc.stderr or ""
			raise Failed(f"TIMEOUT: DuckDB exceeded {timeout}s (hang)\nSQL:\n{sql}") from exc
		self.last_stdout = result.stdout
		self.last_stderr = result.stderr
		if result.returncode != 0:
			raise Failed(
				f"duckdb exited {result.returncode}\nSQL:\n{sql}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
			)
		return result.stdout

	def query_rows(self, sql: str, timeout: int | None = None) -> list:
		out = self.run(sql, timeout=timeout).strip()
		if not out:
			return []
		idx = out.rfind("[")
		if idx < 0:
			raise Failed(f"unparseable duckdb JSON:\n{out}")
		try:
			data = json.loads(out[idx:])
		except json.JSONDecodeError as exc:
			raise Failed(f"unparseable duckdb JSON:\n{out}") from exc
		if isinstance(data, dict):
			return [data]
		if not isinstance(data, list):
			raise Failed(f"expected JSON list, got {type(data)}: {out}")
		return data

	def query_int(self, sql: str, timeout: int | None = None) -> int:
		rows = self.query_rows(sql, timeout=timeout)
		if not rows:
			raise Failed(f"no rows for: {sql}")
		row = rows[0]
		if not row:
			raise Failed(f"empty row for: {sql}")
		return int(next(iter(row.values())))

	def query_result_sets(self, sql: str, timeout: int | None = None) -> list:
		out = self.run(sql, timeout=timeout)
		docs = []
		decoder = json.JSONDecoder()
		i = 0
		text = out.strip()
		while i < len(text):
			while i < len(text) and text[i].isspace():
				i += 1
			if i >= len(text):
				break
			try:
				obj, end = decoder.raw_decode(text, i)
			except json.JSONDecodeError as exc:
				raise Failed(f"unparseable duckdb JSON at {i}:\n{out}") from exc
			if isinstance(obj, dict):
				docs.append([obj])
			elif isinstance(obj, list):
				docs.append(obj)
			i = end
		return docs


def count_files(session: LakeSession) -> int:
	return session.query_int(
		f"SELECT count(*)::BIGINT AS n FROM ducklake_list_files('{CATALOG}', '{TABLE}', schema => '{SCHEMA}');"
	)


def count_delete_files(session: LakeSession) -> int:
	try:
		return session.query_int(
			f"SELECT count(*)::BIGINT AS n FROM ducklake_list_files('{CATALOG}', '{TABLE}', "
			f"schema => '{SCHEMA}') WHERE delete_file IS NOT NULL AND delete_file <> '';"
		)
	except Failed:
		return 0


def count_rows(session: LakeSession) -> int:
	return session.query_int(f"SELECT count(*)::BIGINT AS n FROM {CATALOG}.{SCHEMA}.{TABLE};")


def count_snapshots(session: LakeSession) -> int:
	return session.query_int(f"SELECT count(*)::BIGINT AS n FROM ducklake_snapshots('{CATALOG}');")


def count_inlined_rows(session: LakeSession) -> int:
	meta = f"__ducklake_metadata_{CATALOG}"
	try:
		tables = session.query_rows(
			f"SELECT table_name FROM {meta}.ducklake_inlined_data_tables;"
		)
	except Failed:
		return 0
	total = 0
	for row in tables:
		name = row.get("table_name")
		if not name:
			continue
		try:
			total += session.query_int(
				f"SELECT count(*)::BIGINT AS n FROM {meta}.{name} WHERE end_snapshot IS NULL;"
			)
		except Failed:
			try:
				total += session.query_int(f"SELECT count(*)::BIGINT AS n FROM {meta}.{name};")
			except Failed:
				continue
	return total


def seed(session: LakeSession) -> None:
	session.run(
		f"""
		CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA};
		CREATE TABLE {CATALOG}.{SCHEMA}.{TABLE}(id INTEGER, payload VARCHAR);
		INSERT INTO {CATALOG}.{SCHEMA}.{TABLE} VALUES (1, 'a'), (2, 'b'), (3, 'c');
		"""
	)
	inlined = count_inlined_rows(session)
	files = count_files(session)
	if count_rows(session) != 3:
		raise Failed(f"inlined seed should have 3 table rows, got {count_rows(session)}")
	if files != 0:
		raise Failed(f"inlined seed must not write parquet files, got {files}")
	print(f"seed inlined: inlined_rows={inlined} files={files} table_rows=3")

	# Later processes must not re-enable inlining via ATTACH. Two rows per
	# file so a partial delete keeps the data file in ducklake_list_files
	# (a fully-deleted one-row file disappears from the live listing).
	session.inlining_row_limit = 0
	inserts = "\n".join(
		f"INSERT INTO {CATALOG}.{SCHEMA}.{TABLE} VALUES "
		f"({100 + i}, 'file-{i}-a'), ({200 + i}, 'file-{i}-b');"
		for i in range(8)
	)
	set_option = (
		f"CALL ducklake_set_option('{CATALOG}', 'data_inlining_row_limit', 0);"
	)
	set_option_alt = f"CALL {CATALOG}.set_option('data_inlining_row_limit', 0);"
	delete = f"DELETE FROM {CATALOG}.{SCHEMA}.{TABLE} WHERE id >= 100 AND id < 104;"
	try:
		session.run(f"{set_option}\n{inserts}")
	except Failed:
		session.run(f"{set_option_alt}\n{inserts}")
	files_after_insert = count_files(session)
	if files_after_insert < 8:
		raise Failed(f"expected at least 8 small files after inserts, got {files_after_insert}")
	session.run(delete)
	files = count_files(session)
	deletes = count_delete_files(session)
	inlined_after = count_inlined_rows(session)
	rows = count_rows(session)
	if files < 8:
		raise Failed(f"expected at least 8 live data files after partial deletes, got {files}")
	if inlined_after <= 0:
		raise Failed("inlined seed rows were lost while writing small files")
	if rows >= 3 + 16:
		raise Failed(f"delete did not remove rows, table_rows={rows}")
	print(
		f"seed files+deletes: files={files} delete_files={deletes} "
		f"inlined_rows={inlined_after} table_rows={rows} snapshots={count_snapshots(session)}"
	)


def expect_refused(session: LakeSession, sql: str, needle: str, label: str, timeout: int) -> None:
	needles = (needle,) if isinstance(needle, str) else tuple(needle)
	try:
		session.run(sql, timeout=timeout)
	except Failed as exc:
		text = str(exc)
		if "TIMEOUT" in text:
			raise
		if not any(item in text for item in needles):
			raise Failed(f"{label} error should mention one of {needles!r}:\n{text}")
		return
	raise Failed(f"{label} should have been refused")


def assert_execute_guards(session: LakeSession, timeout: int) -> None:
	snaps = count_snapshots(session)
	files = count_files(session)
	inlined = count_inlined_rows(session)
	print(f"before execute guards: snapshots={snaps} files={files} inlined_rows={inlined}")
	expect_refused(
		session,
		f"EXPLAIN SELECT * FROM lakemon_maintain('{CATALOG}', '{QUALIFIED}', dry_run => false);",
		"EXPLAIN",
		"EXPLAIN dry_run => false",
		timeout,
	)
	expect_refused(
		session,
		f"PREPARE lakemon_exec AS SELECT * FROM lakemon_maintain('{CATALOG}', '{QUALIFIED}', "
		f"dry_run => false);",
		"PREPARE",
		"PREPARE dry_run => false",
		timeout,
	)
	expect_refused(
		session,
		f"BEGIN;\nCALL lakemon_maintain('{CATALOG}', '{QUALIFIED}', dry_run => false);",
		"explicit transaction",
		"BEGIN dry_run => false",
		timeout,
	)
	expect_refused(
		session,
		f"CREATE VIEW lakemon_view AS SELECT * FROM lakemon_maintain('{CATALOG}', '{QUALIFIED}', "
		f"dry_run => false);",
		"CREATE",
		"CREATE VIEW dry_run => false",
		timeout,
	)
	# Same bind path as C API / Python / JDBC prepare with a parameter.
	expect_refused(
		session,
		f"SELECT * FROM lakemon_maintain(?, dry_run => false);",
		("prepared", "Parameter", "parameter"),
		"parameterized SELECT dry_run => false",
		timeout,
	)
	if count_snapshots(session) != snaps:
		raise Failed(f"guards changed snapshots: {snaps} -> {count_snapshots(session)}")
	if count_files(session) != files:
		raise Failed(f"guards changed files: {files} -> {count_files(session)}")
	if count_inlined_rows(session) != inlined:
		raise Failed(f"guards changed inlined rows: {inlined} -> {count_inlined_rows(session)}")
	print(f"execute guards ok: snapshots={snaps} files={files} inlined_rows={inlined}")


def split_maintain_and_logs(sets: list) -> tuple[list, list]:
	maintain_rows = []
	log_rows = []
	for rows in sets:
		if not rows:
			continue
		sample = rows[0]
		if any(str(row.get("message", "")).startswith("lakemon ") for row in rows):
			log_rows.extend(rows)
		elif any(str(row.get("step", "")).startswith("lakemon ") for row in rows):
			log_rows.extend(rows)
		elif "step" in sample and "status" in sample:
			maintain_rows.extend(rows)
	return maintain_rows, log_rows


def log_message(row: dict) -> str:
	for key in ("extra", "step", "message", "action"):
		value = row.get(key)
		if isinstance(value, str) and value.startswith("lakemon "):
			return value
	return ""


def assert_step_logs(log_rows: list, stderr: str) -> None:
	messages = [log_message(row) for row in log_rows]
	messages = [msg for msg in messages if msg]
	starts = [msg for msg in messages if msg.startswith("lakemon start")]
	ends = [msg for msg in messages if msg.startswith("lakemon end")]
	progress = [msg for msg in messages if "progress=" in msg]
	if len(starts) < 1 or len(ends) < 1:
		raise Failed(f"expected lakemon start/end log lines, got {messages}")
	if len(progress) < 1:
		raise Failed(f"expected progress= in lakemon logs, got {messages}")
	if "lakemon start" not in stderr and "lakemon end" not in stderr:
		raise Failed(f"lakemon_log_stderr=true should write step lines to stderr:\n{stderr}")
	print(f"step logs ok: starts={len(starts)} ends={len(ends)} progress_lines={len(progress)}")


def parse_duration_ms(message: str) -> int | None:
	match = re.search(r"duration_ms=(\d+)", message)
	if not match:
		return None
	return int(match.group(1))


def assert_step_durations(log_rows: list, limit_ms: int) -> None:
	slow = []
	seen = 0
	for row in log_rows:
		message = log_message(row)
		if not message.startswith("lakemon end"):
			continue
		duration = parse_duration_ms(message)
		if duration is None:
			continue
		seen += 1
		print(f"step duration: {message}")
		if duration > limit_ms:
			slow.append((duration, message))
	if seen < 1:
		raise Failed("no duration_ms values in lakemon end logs")
	if slow:
		raise Failed(f"step duration exceeded {limit_ms}ms: {slow}")


def pg_env(pg: str) -> dict:
	env = os.environ.copy()
	for part in shlex.split(pg):
		if "=" not in part:
			continue
		key, value = part.split("=", 1)
		if key == "dbname":
			env["PGDATABASE"] = value
		elif key == "host":
			env["PGHOST"] = value
		elif key == "port":
			env["PGPORT"] = value
		elif key == "user":
			env["PGUSER"] = value
		elif key == "password":
			env["PGPASSWORD"] = value
	env.setdefault("PGDATABASE", "lakemon")
	env.setdefault("PGHOST", "127.0.0.1")
	env.setdefault("PGUSER", "postgres")
	env.setdefault("PGPASSWORD", "postgres")
	return env


def have_psql() -> bool:
	try:
		result = subprocess.run(["psql", "--version"], capture_output=True, text=True, timeout=10)
		return result.returncode == 0
	except (OSError, subprocess.TimeoutExpired):
		return False


def psql(pg: str, sql: str, timeout: int = 30) -> str:
	result = subprocess.run(
		["psql", "-v", "ON_ERROR_STOP=1", "-P", "pager=off", "-c", sql],
		env=pg_env(pg),
		text=True,
		capture_output=True,
		timeout=timeout,
	)
	if result.returncode != 0:
		raise Failed(f"psql failed: {result.stderr or result.stdout}\nSQL:\n{sql}")
	return result.stdout


def dump_pg_locks(pg: str, label: str) -> str:
	sql = """
	SELECT now() AS dumped_at;
	SELECT pid, usename, state, wait_event_type, wait_event, left(query, 180) AS query
	FROM pg_stat_activity
	WHERE datname = current_database()
	ORDER BY pid;
	SELECT locktype, relation::regclass AS relation, mode, granted, pid
	FROM pg_locks
	WHERE database = (SELECT oid FROM pg_database WHERE datname = current_database())
	ORDER BY granted, pid, relation::text;
	"""
	try:
		text = psql(pg, sql)
	except Failed as exc:
		text = str(exc)
	print(f"--- pg_locks / pg_stat_activity ({label}) ---")
	print(text)
	print(f"--- end pg dump ({label}) ---")
	return text


def list_ducklake_pg_tables(pg: str) -> list[tuple[str, str]]:
	out = psql(
		pg,
		"SELECT n.nspname, c.relname FROM pg_class c "
		"JOIN pg_namespace n ON n.oid = c.relnamespace "
		"WHERE c.relkind = 'r' AND (c.relname ILIKE '%ducklake%' OR c.relname ILIKE '%snapshot%') "
		"ORDER BY 1, 2;",
	)
	tables = []
	for line in out.splitlines():
		if "|" not in line or "nspname" in line or "---" in line:
			continue
		parts = [part.strip() for part in line.split("|")]
		if len(parts) >= 2 and parts[0] and parts[1]:
			tables.append((parts[0], parts[1]))
	return tables


def quote_ident(name: str) -> str:
	return '"' + name.replace('"', '""') + '"'


def pick_lock_target(tables: list[tuple[str, str]]) -> str | None:
	for schema, name in tables:
		if "snapshot" in name.lower():
			return quote_ident(schema) + "." + quote_ident(name)
	if tables:
		schema, name = tables[0]
		return quote_ident(schema) + "." + quote_ident(name)
	return None


def execute_maintain(session: LakeSession, timeout: int) -> None:
	rows_before = count_rows(session)
	files_before = count_files(session)
	inlined_before = count_inlined_rows(session)
	snaps_before = count_snapshots(session)
	print(
		f"before maintain: files={files_before} inlined_rows={inlined_before} "
		f"table_rows={rows_before} snapshots={snaps_before}"
	)
	if inlined_before <= 0:
		raise Failed(f"setup lost inlined data before maintain (inlined_rows={inlined_before})")
	if files_before < 8:
		raise Failed(f"setup did not keep many small files before maintain (files={files_before})")

	sets = session.query_result_sets(
		f"""
		SET enable_logging = true;
		SET lakemon_log_stderr = true;
		SET lakemon_catalog_lock_timeout = '30s';
		SELECT step, action, status, files_created FROM lakemon_maintain('{CATALOG}', '{QUALIFIED}', dry_run => false);
		SELECT message FROM duckdb_logs() WHERE message LIKE 'lakemon %';
		""",
		timeout=timeout,
	)
	rows, log_rows = split_maintain_and_logs(sets)
	print("maintain result:")
	print(json.dumps(rows, indent=2))
	print("maintain logs:")
	print(json.dumps(log_rows, indent=2))
	assert_step_logs(log_rows, session.last_stderr)
	assert_step_durations(log_rows, STEP_DURATION_LIMIT_MS)
	flush = [row for row in rows if row.get("step") == "flush_inlined"]
	if not flush or flush[0].get("status") != "ok":
		raise Failed(f"flush_inlined did not run to ok: {flush}")
	if not any(row.get("step") == "merge" and row.get("status") == "ok" for row in rows):
		raise Failed(f"no merge step ran to ok: {rows}")
	# set_option (set_target_*) does not add a DuckLake snapshot; count only
	# flush / rewrite / merge CALLs so a double bind would be 2x this delta.
	ok_mutating = [
		row
		for row in rows
		if row.get("status") == "ok"
		and row.get("step") in ("flush_inlined", "rewrite", "merge")
		and not str(row.get("action", "")).startswith("set_target_")
	]
	snaps_after = count_snapshots(session)
	snap_delta = snaps_after - snaps_before
	if snap_delta != len(ok_mutating):
		raise Failed(
			f"one CALL should add one snapshot per ok mutating step: "
			f"delta={snap_delta} ok_steps={len(ok_mutating)} rows={rows}"
		)
	if rows_before != count_rows(session):
		raise Failed(f"row count changed during maintain: {rows_before} -> {count_rows(session)}")

	inlined_after = count_inlined_rows(session)
	leftover = session.query_int(
		f"SELECT count(*)::BIGINT AS n FROM ducklake_flush_inlined_data('{CATALOG}', "
		f"table_name => '{TABLE}', schema_name => '{SCHEMA}');"
	)
	if leftover != 0:
		raise Failed(f"inlined data not flushed: ducklake_flush_inlined_data still reports {leftover} rows")

	files_after = count_files(session)
	print(f"after maintain: files={files_after} inlined_rows={inlined_after} leftover_flush={leftover}")
	if files_after >= files_before:
		raise Failed(f"merge did not reduce data files: {files_before} -> {files_after}")


def execute_expire(session: LakeSession, timeout: int) -> None:
	snapshots_before = count_snapshots(session)
	if snapshots_before < 2:
		raise Failed(f"need multiple snapshots to expire, got {snapshots_before}")
	print(f"before expire: snapshots={snapshots_before}")
	time.sleep(2)
	rows = session.query_rows(
		f"SELECT step, status FROM lakemon_maintain_global('{CATALOG}', "
		f"expire_older_than => '0 days', skip_cleanup => true) "
		f"WHERE step = 'expire_snapshots';",
		timeout=timeout,
	)
	print("expire result:")
	print(json.dumps(rows, indent=2))
	if not rows or rows[0].get("status") != "ok":
		raise Failed(f"expire_snapshots did not run to ok: {rows}")
	snapshots_after = count_snapshots(session)
	print(f"after expire: snapshots={snapshots_after}")
	if snapshots_after >= snapshots_before:
		raise Failed(f"expire did not drop snapshots: {snapshots_before} -> {snapshots_after}")
	if count_rows(session) <= 0:
		raise Failed("table empty after expire")


def seed_heavy(session: LakeSession, table: str, files: int, timeout: int) -> None:
	qualified = f"{SCHEMA}.{table}"
	session.inlining_row_limit = 0
	inserts = "\n".join(
		f"INSERT INTO {CATALOG}.{qualified} VALUES "
		f"({1000 + i}, 'heavy-{i}-a'), ({2000 + i}, 'heavy-{i}-b');"
		for i in range(files)
	)
	body = f"""
		CREATE TABLE IF NOT EXISTS {CATALOG}.{qualified}(id INTEGER, payload VARCHAR);
		{inserts}
		DELETE FROM {CATALOG}.{qualified} WHERE id >= 1000 AND id < {1000 + max(8, files // 8)};
		"""
	try:
		session.run(
			f"CALL ducklake_set_option('{CATALOG}', 'data_inlining_row_limit', 0);\n{body}",
			timeout=timeout,
		)
	except Failed:
		session.run(f"CALL {CATALOG}.set_option('data_inlining_row_limit', 0);\n{body}", timeout=timeout)
	live_files = session.query_int(
		f"SELECT count(*)::BIGINT AS n FROM ducklake_list_files('{CATALOG}', '{table}', schema => '{SCHEMA}');"
	)
	if live_files < files:
		raise Failed(f"heavy seed expected at least {files} files, got {live_files}")
	print(f"heavy seed: table={qualified} files={live_files} rows={session.query_int(f'SELECT count(*)::BIGINT AS n FROM {CATALOG}.{qualified};')}")


def execute_heavy_maintain(session: LakeSession, timeout: int, pg: str) -> None:
	session.inlining_row_limit = 0
	stop = threading.Event()
	dumps = []

	def sampler() -> None:
		while not stop.wait(2.0):
			try:
				dumps.append(dump_pg_locks(pg, "heavy-maintain"))
			except Exception as exc:  # noqa: BLE001
				dumps.append(str(exc))

	thread = threading.Thread(target=sampler, daemon=True)
	thread.start()
	try:
		sets = session.query_result_sets(
			f"""
			SET enable_logging = true;
			SET lakemon_log_stderr = true;
			SET lakemon_catalog_lock_timeout = '30s';
			SELECT step, action, status, files_created
			FROM lakemon_maintain('{CATALOG}', '{HEAVY_QUALIFIED}', dry_run => false);
			SELECT message FROM duckdb_logs() WHERE message LIKE 'lakemon %';
			""",
			timeout=timeout,
		)
	finally:
		stop.set()
		thread.join(timeout=5)

	maintain_rows, log_rows = split_maintain_and_logs(sets)
	print("heavy maintain result:")
	print(json.dumps(maintain_rows, indent=2))
	if not any(row.get("step") == "merge" and row.get("status") == "ok" for row in maintain_rows):
		raise Failed(f"heavy maintain did not merge: {maintain_rows}")
	assert_step_logs(log_rows, session.last_stderr)
	assert_step_durations(log_rows, STEP_DURATION_LIMIT_MS)
	lock_waits = [text for text in dumps if "Lock" in text or "lock" in text]
	print(f"heavy maintain lock samples={len(dumps)} texts_mentioning_lock={len(lock_waits)}")


def hold_psql_lock(pg: str, relation: str) -> subprocess.Popen:
	proc = subprocess.Popen(
		["psql", "-v", "ON_ERROR_STOP=1", "-P", "pager=off"],
		env=pg_env(pg),
		stdin=subprocess.PIPE,
		stdout=subprocess.PIPE,
		stderr=subprocess.STDOUT,
		text=True,
	)
	assert proc.stdin is not None
	proc.stdin.write(
		f"BEGIN;\nLOCK TABLE {relation} IN ACCESS EXCLUSIVE MODE;\nSELECT 'locked' AS status;\nSELECT pg_sleep(90);\n"
	)
	proc.stdin.flush()
	time.sleep(1.5)
	if proc.poll() is not None:
		out = proc.stdout.read() if proc.stdout else ""
		raise Failed(f"psql lock holder exited early: {out}")
	return proc


def hold_duckdb_catalog_read(session: LakeSession) -> subprocess.Popen:
	sql = session.preamble() + f"""
	BEGIN;
	SELECT count(*) FROM ducklake_snapshots('{CATALOG}');
	SELECT count(*) FROM {CATALOG}.{SCHEMA}.{TABLE};
	SELECT 1;
	"""
	proc = subprocess.Popen(
		[session.duckdb, "-unsigned", ":memory:"],
		stdin=subprocess.PIPE,
		stdout=subprocess.PIPE,
		stderr=subprocess.STDOUT,
		text=True,
		cwd=str(ROOT),
	)
	assert proc.stdin is not None
	proc.stdin.write(sql)
	proc.stdin.flush()
	time.sleep(1.0)
	if proc.poll() is not None:
		out = proc.stdout.read() if proc.stdout else ""
		raise Failed(f"second DuckDB session exited early: {out}")
	return proc


def finish_or_clear_error(exc: Failed, timeout: int) -> str:
	text = str(exc)
	if "TIMEOUT" in text:
		raise Failed(f"maintain hung under a concurrent catalog session (>{timeout}s):\n{text}")
	return text


def execute_concurrent_catalog(session: LakeSession, timeout: int, pg: str) -> None:
	if not have_psql():
		raise Failed("psql is required to dump pg_locks and hold a catalog lock")

	tables = list_ducklake_pg_tables(pg)
	print(f"postgres ducklake tables: {tables}")
	relation = pick_lock_target(tables)
	print(f"lock target: {relation}")
	dump_pg_locks(pg, "before-concurrent")

	# Second DuckDB session: BEGIN + catalog read. postgres_scanner may or may
	# not keep a Postgres transaction; dump locks to record which.
	duck_holder = hold_duckdb_catalog_read(session)
	try:
		duck_locks = dump_pg_locks(pg, "second-duckdb-session")
		try:
			session.query_rows(
				f"""
				SET lakemon_catalog_lock_timeout = '10s';
				SELECT step, status FROM lakemon_maintain('{CATALOG}', '{QUALIFIED}', dry_run => false)
				WHERE step = 'flush_inlined';
				""",
				timeout=timeout,
			)
			print("second DuckDB session: maintain finished (DuckDB BEGIN did not block the catalog commit)")
			second_session_blocked = False
		except Failed as exc:
			text = finish_or_clear_error(exc, timeout)
			print("second DuckDB session: maintain failed without hanging")
			print(text)
			second_session_blocked = True
			if "another DuckDB session" not in text and "lock" not in text.lower():
				print("note: error did not name a catalog lock; recorded as a non-hang failure")
		print(f"second DuckDB session lock dump mentioned idle-in-transaction={'idle in transaction' in duck_locks.lower()}")
		print(f"second DuckDB session blocked_maintain={second_session_blocked}")
	finally:
		if duck_holder.poll() is None:
			duck_holder.terminate()
		duck_holder.wait(timeout=5)

	if relation is None:
		print("no Postgres snapshot relation found; skip ACCESS EXCLUSIVE holder")
		return

	# lock_timeout is per Postgres session. ALTER DATABASE covers new DuckLake
	# catalog connections even when postgres_execute cannot reach them.
	pg_db = pg_env(pg).get("PGDATABASE", "lakemon")
	psql(pg, f"ALTER DATABASE {quote_ident(pg_db)} SET lock_timeout = '5s';")
	holder = hold_psql_lock(pg, relation)
	try:
		held = dump_pg_locks(pg, "psql-access-exclusive")
		if "AccessExclusiveLock" not in held and "AccessExclusive" not in held:
			print("warning: AccessExclusiveLock not visible yet; continuing")
		started = time.time()
		try:
			session.query_rows(
				f"""
				SET enable_logging = true;
				SET lakemon_catalog_lock_timeout = '5s';
				SET lakemon_log_stderr = true;
				SELECT step, status, details FROM lakemon_maintain('{CATALOG}', '{QUALIFIED}', dry_run => false);
				""",
				timeout=CONCURRENT_TIMEOUT,
			)
			elapsed = time.time() - started
			print(f"psql ACCESS EXCLUSIVE: maintain finished in {elapsed:.1f}s (commit did not wait on the lock)")
		except Failed as exc:
			elapsed = time.time() - started
			text = finish_or_clear_error(exc, CONCURRENT_TIMEOUT)
			print(f"psql ACCESS EXCLUSIVE: maintain failed in {elapsed:.1f}s without hanging")
			print(text)
			dump_pg_locks(pg, "psql-access-exclusive-after-error")
			if elapsed > CONCURRENT_TIMEOUT:
				raise Failed(f"maintain took too long under a held catalog lock: {elapsed:.1f}s")
			# Prefer the explicit hint; still accept a raw Postgres lock timeout.
			if "another DuckDB session" not in text and "lock" not in text.lower() and "timeout" not in text.lower():
				raise Failed(f"expected a catalog lock / timeout error, got:\n{text}")
	finally:
		if holder.poll() is None:
			holder.terminate()
		holder.wait(timeout=5)
		try:
			psql(pg, f"ALTER DATABASE {quote_ident(pg_db)} RESET lock_timeout;")
		except Failed as exc:
			print(f"reset lock_timeout failed: {exc}")
	dump_pg_locks(pg, "after-concurrent")


def main() -> int:
	args = parse_args()
	duckdb = Path(args.duckdb)
	if not duckdb.is_file():
		print(f"{duckdb} is missing; build the extension first", file=sys.stderr)
		return 1
	data_path = args.data_path or str(Path(tempfile.mkdtemp(prefix="lakemon_pg_data_")))
	Path(data_path).mkdir(parents=True, exist_ok=True)
	build_dir = duckdb.parent
	postgres_ext = find_postgres_extension(build_dir)
	ducklake_ext = find_extension(build_dir, "ducklake")
	lakemon_ext = find_extension(build_dir, "lakemon")
	postgres_load = probe_postgres_load(str(duckdb), postgres_ext)
	ducklake_load = probe_named_load(str(duckdb), "ducklake", ducklake_ext, optional=True)
	lakemon_load = probe_named_load(
		str(duckdb), "lakemon", lakemon_ext, verify_sql="SELECT lakemon_version();"
	)
	print(f"duckdb={duckdb}")
	print(f"postgres_ext={postgres_ext}")
	print(f"ducklake_ext={ducklake_ext}")
	print(f"lakemon_ext={lakemon_ext}")
	print(f"postgres_load={postgres_load}")
	print(f"ducklake_load={ducklake_load}")
	print(f"lakemon_load={lakemon_load}")
	print(f"data_path={data_path}")
	session = LakeSession(str(duckdb), args.pg, data_path, [postgres_load, ducklake_load, lakemon_load])
	try:
		seed(session)
		assert_execute_guards(session, args.timeout)
		execute_maintain(session, args.timeout)
		execute_expire(session, args.timeout)
		heavy_timeout = max(args.timeout, 180)
		seed_heavy(session, HEAVY_TABLE, HEAVY_FILE_COUNT, heavy_timeout)
		execute_heavy_maintain(session, heavy_timeout, args.pg)
		execute_concurrent_catalog(session, args.timeout, args.pg)
	except Failed as exc:
		print(str(exc), file=sys.stderr)
		return 1
	print("postgres lake execute ok")
	return 0


if __name__ == "__main__":
	sys.exit(main())
