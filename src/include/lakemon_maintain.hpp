#pragma once

#include "duckdb.hpp"
#include "lakemon_sql.hpp"

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
};

std::vector<MaintainRow> RunMaintain(duckdb::ClientContext &context, const MaintainOptions &options);
std::vector<MaintainRow> RunGlobalMaintain(duckdb::ClientContext &context, const GlobalMaintainOptions &options);

// PRAGMA replacement: planned DuckLake statements (when execute) plus a final
// SELECT of step rows. DuckDB runs that string as the query itself.
std::string ExecuteScriptFromRows(const std::vector<MaintainRow> &rows, bool execute);

// Table-function bind_replace result: step-row VALUES. Execute is
// ExecutePlannedRows (one committed statement per DuckLake CALL) because a
// DuckLake transaction cannot mix data changes and compaction.
std::string BindReplaceQuery(const std::vector<MaintainRow> &rows, bool execute);

// Run each non-empty row.sql on a nested Connection (own commit). Fail-fast:
// first error sets that row to status=error and leaves later SQL unrun.
void ExecutePlannedRows(duckdb::ClientContext &context, std::vector<MaintainRow> &rows);

} // namespace lakemon
