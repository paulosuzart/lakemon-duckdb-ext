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
// SELECT of step rows. Table functions never run these statements themselves.
std::string ExecuteScriptFromRows(const std::vector<MaintainRow> &rows, bool execute);

} // namespace lakemon
