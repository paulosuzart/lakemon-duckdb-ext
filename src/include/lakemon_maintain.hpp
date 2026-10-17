#pragma once

#include "duckdb.hpp"
#include "lakemon_sql.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace lakemon {

// Table-scoped maintain: inventory, flush, adaptive rewrite, merge.
struct MaintainOptions {
	std::string catalog;
	TableRef table;
	bool dry_run = false;
	int64_t max_compacted_files = 0;
};

// Catalog-global retention: expire snapshots, old-file cleanup, orphan delete.
struct GlobalMaintainOptions {
	std::string catalog;
	bool dry_run = false;
	bool skip_expire = false;
	bool skip_cleanup = false;
	std::string expire_older_than;
	std::string delete_older_than;
	bool expire_older_than_set = false;
	bool delete_older_than_set = false;
};

// Planned SQL that ExecutePlannedRows must not run unless ducklake is loaded.
enum class PlannedCall { None, DuckLake };

struct MaintainRow {
	std::string step;
	std::string schema_name;
	std::string table_name;
	std::string action;
	std::string status;
	int64_t files_processed = 0;
	int64_t files_created = 0;
	std::string details;
	// DuckLake / catalog SQL for this step. Empty for skip / inventory-only rows.
	std::string sql;
	PlannedCall planned_call = PlannedCall::None;
	// Byte weight for progress logs. 0 means a unit weight (see StepWeight).
	uint64_t planned_bytes = 0;
	int64_t duration_ms = 0;
};

std::vector<MaintainRow> RunMaintain(duckdb::ClientContext &context, const MaintainOptions &options);
std::vector<MaintainRow> RunGlobalMaintain(duckdb::ClientContext &context, const GlobalMaintainOptions &options);

// Table-function bind_replace / PRAGMA result: step-row VALUES. Execute is
// ExecutePlannedRows (one committed statement per DuckLake CALL) because a
// DuckLake transaction cannot mix data changes and compaction.
std::string BindReplaceQuery(const std::vector<MaintainRow> &rows, bool execute);

// Refuse dry_run => false unless the statement is a plain CALL / SELECT /
// FROM / WITH / PRAGMA in autocommit. Also refuses an explicit BEGIN and a
// bind that has prepared-statement parameters (C API / driver prepare).
// statement_override is the SQL used for the keyword check. Pass "PRAGMA"
// from the PRAGMA query function: GetCurrentQuery() needs active_query, which
// is not set while DuckDB is still rewriting the PRAGMA.
void RequireMaintainExecute(duckdb::ClientContext &context, bool has_prepared_parameters = false,
                            const char *statement_override = nullptr);

// Run each non-empty row.sql on a nested Connection (own commit). Fail-fast:
// first error appends to details, marks later SQL rows skip / not-run, and
// sets files_created from the CALL result row count. Interrupt rethrows so
// the result rows are not returned; already-committed steps stay committed.
// Logs start/end (step, table, duration, files, error, progress) to DuckDB
// logging and optionally stderr.
void ExecutePlannedRows(duckdb::ClientContext &context, std::vector<MaintainRow> &rows);

} // namespace lakemon
