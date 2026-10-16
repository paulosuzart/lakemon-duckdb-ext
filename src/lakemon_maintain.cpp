#include "lakemon_maintain.hpp"

#include "lakemon_catalog.hpp"
#include "lakemon_compat.hpp"
#include "lakemon_pipeline.hpp"
#include "lakemon_policy.hpp"
#include "lakemon_store.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

#include <exception>
#include <new>
#include <sstream>

namespace lakemon {

using duckdb::Exception;

static MaintainRow MakeRow(const std::string &step, const std::string &schema, const std::string &table,
                           const std::string &action, const std::string &status, int64_t processed, int64_t created,
                           const std::string &details) {
	MaintainRow row;
	row.step = step;
	row.schema_name = schema;
	row.table_name = table;
	row.action = action;
	row.status = status;
	row.files_processed = processed;
	row.files_created = created;
	row.details = details;
	return row;
}

static void AppendPlanned(std::vector<MaintainRow> &rows, const std::string &step, const std::string &action,
                          const std::string &sql, const std::string &schema, const std::string &table,
                          const std::string &details, int64_t processed = 0) {
	MaintainRow row = MakeRow(step, schema, table, action, "planned", processed, 0, details + " | " + sql);
	row.sql = sql;
	rows.push_back(std::move(row));
}

static std::string PlanRowsSelect(const std::vector<MaintainRow> &rows, bool executed) {
	if (rows.empty()) {
		return "SELECT 'inventory' AS step, '' AS schema_name, '' AS table_name, 'catalog_select' AS action, "
		       "'skip' AS status, 0::BIGINT AS files_processed, 0::BIGINT AS files_created, "
		       "'no steps' AS details WHERE false";
	}
	std::ostringstream sql;
	sql << "SELECT * FROM (VALUES ";
	for (size_t i = 0; i < rows.size(); i++) {
		const auto &row = rows[i];
		const std::string status = (executed && !row.sql.empty()) ? "ok" : row.status;
		if (i > 0) {
			sql << ", ";
		}
		sql << "(" << QuoteString(row.step) << ", " << QuoteString(row.schema_name) << ", "
		    << QuoteString(row.table_name) << ", " << QuoteString(row.action) << ", " << QuoteString(status) << ", "
		    << row.files_processed << "::BIGINT, " << row.files_created << "::BIGINT, " << QuoteString(row.details)
		    << ")";
	}
	sql << ") AS t(step, schema_name, table_name, action, status, files_processed, files_created, details)";
	return sql.str();
}

static void AppendRewriteSkips(std::vector<MaintainRow> &rows, const policy::TableHint &hint, const std::string &reason) {
	if (hint.rewrite_steps.empty()) {
		rows.push_back(MakeRow("rewrite", hint.schema_name, hint.table_name, "none", "skip",
		                       static_cast<int64_t>(hint.file_count), 0, reason));
		return;
	}
	for (const auto &step : hint.rewrite_steps) {
		rows.push_back(MakeRow("rewrite", hint.schema_name, hint.table_name, step.band, "skip",
		                       static_cast<int64_t>(step.files.size()), 0,
		                       policy::FormatRewriteStepDetails(step) + " | " + reason));
	}
}

std::string BindReplaceQuery(const std::vector<MaintainRow> &rows, bool execute) {
	return PlanRowsSelect(rows, execute);
}

void RequireMaintainExecute(duckdb::ClientContext &context, bool has_prepared_parameters,
                            const char *statement_override) {
	std::string error;
	std::string statement;
	if (statement_override) {
		statement = statement_override;
	} else {
		// GetCurrentQuery() dereferences active_query. PRAGMA rewrite runs in
		// ParseStatementsInternal before BeginQueryInternal, so active_query is
		// null and unique_ptr throws INTERNAL (invalidates the database).
		try {
			statement = context.GetCurrentQuery();
		} catch (const duckdb::InternalException &) {
			statement.clear();
		}
	}
	if (MaintainExecuteRefused(context.transaction.IsAutoCommit(), statement, error, has_prepared_parameters)) {
		throw duckdb::InvalidInputException("%s", error.c_str());
	}
}

static void AppendDetails(MaintainRow &row, const std::string &suffix) {
	if (row.details.empty()) {
		row.details = suffix;
		return;
	}
	row.details += " | ";
	row.details += suffix;
}

static bool DuckLakeFunctionRegistered(duckdb::ClientContext &context) {
	if (!context.db) {
		return false;
	}
	try {
		duckdb::Connection con(*context.db);
		auto result =
		    con.Query("SELECT 1 FROM duckdb_functions() WHERE function_name = 'ducklake_expire_snapshots' LIMIT 1");
		if (!result || result->HasError()) {
			return false;
		}
		return result->RowCount() > 0;
	} catch (const duckdb::InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const Exception &) {
		return false;
	} catch (const std::exception &) {
		return false;
	}
}

void ExecutePlannedRows(duckdb::ClientContext &context, std::vector<MaintainRow> &rows) {
	bool failed = false;
	bool interrupted = false;
	bool ducklake_checked = false;
	bool ducklake_present = true;
	for (auto &row : rows) {
		if (row.sql.empty()) {
			continue;
		}
		if (failed) {
			row.status = "skip";
			AppendDetails(row, interrupted ? "not run: interrupted" : "not run: previous step failed");
			continue;
		}
		if (row.sql.find("ducklake_") != std::string::npos) {
			if (!ducklake_checked) {
				ducklake_present = DuckLakeFunctionRegistered(context);
				ducklake_checked = true;
			}
			if (!ducklake_present) {
				row.status = "error";
				AppendDetails(row, "lakemon: ducklake is not loaded");
				failed = true;
				continue;
			}
		}
		try {
			auto result = RunSQL(context, row.sql);
			row.status = "ok";
			if (result) {
				row.files_created = ResultRowCount(*result);
			}
		} catch (const duckdb::InterruptException &) {
			row.status = "error";
			AppendDetails(row, "interrupted");
			failed = true;
			interrupted = true;
		} catch (const std::bad_alloc &) {
			throw;
		} catch (const Exception &ex) {
			row.status = "error";
			AppendDetails(row, SafeWhat(ex));
			failed = true;
		} catch (const std::exception &ex) {
			row.status = "error";
			AppendDetails(row, SafeWhat(ex));
			failed = true;
		}
	}
	if (interrupted) {
		throw duckdb::InterruptException();
	}
}

// Inventory hard-failure: status=error row(s), then stop (do not plan rewrite/merge
// against an unread catalog). Unread per-table metadata: status=error diagnostic
// rows; continue with tables that were read. This function only plans: flush,
// rewrite, and merge SQL is returned as status=planned. bind_replace (CALL /
// SELECT) and PRAGMA run each CALL as its own committed statement after
// RequireMaintainExecute. Expire / old-file / orphan cleanup are not part of
// this plan (see RunGlobalMaintain). InterruptException and bad_alloc rethrow.
// The session stays usable.
std::vector<MaintainRow> RunMaintain(duckdb::ClientContext &context, const MaintainOptions &options) {
	std::vector<MaintainRow> rows;
	std::vector<policy::FileStat> files;
	std::vector<policy::TableHint> hints;
	std::vector<InventoryDiagnostic> diagnostics;
	std::string options_error;
	std::string policy_error;
	std::vector<policy::OptionBinding> catalog_options;
	policy::ActivePolicy active = policy::DefaultPolicy();
	try {
		hints = InventoryTables(context, options.catalog, options.table, &diagnostics, &files);
		catalog_options = LoadCatalogOptions(context, options.catalog, &options_error);
		active = LoadActivePolicy(context, options.catalog, &policy_error);
	} catch (const duckdb::InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const Exception &ex) {
		rows.push_back(MakeRow("inventory", options.table.schema, options.table.table, "catalog_select", "error", 0, 0,
		                       SafeWhat(ex)));
		return rows;
	} catch (const std::exception &ex) {
		rows.push_back(MakeRow("inventory", options.table.schema, options.table.table, "catalog_select", "error", 0, 0,
		                       SafeWhat(ex)));
		return rows;
	}

	try {
	for (const auto &diag : diagnostics) {
		rows.push_back(MakeRow("inventory", diag.schema_name, diag.table_name, diag.source, "error", 0, 0, diag.message));
	}
	if (hints.empty()) {
		if (diagnostics.empty()) {
			rows.push_back(MakeRow("inventory", options.table.schema, options.table.table, "catalog_select", "skip", 0,
			                       0, "no DuckLake tables matched"));
		}
		return rows;
	}

	rows.push_back(MakeRow("inventory", "", "", "catalog_select", "ok", static_cast<int64_t>(files.size()),
	                       static_cast<int64_t>(hints.size()),
	                       "byte-weighted adaptive rewrite ladder + merge size bands"));
	if (!options_error.empty()) {
		rows.push_back(MakeRow("catalog_options", "", "", "ducklake_options", "error", 0, 0, options_error));
	}
	if (!policy_error.empty()) {
		rows.push_back(MakeRow("lakemon_policy", "", "", "persisted_overrides", "error", 0, 0, policy_error));
	}

	const bool catalog_wide = options.table.table.empty();

	AppendPlanned(rows, "flush_inlined", "ducklake_flush_inlined_data",
	              FlushInlinedDataCall(options.catalog, options.table), options.table.schema, options.table.table,
	              "flush inlined rows before rewrite/merge");

	for (const auto &hint : hints) {
		TableRef ref;
		ref.schema = hint.schema_name;
		ref.table = hint.table_name;
		if (policy::SkipForAutoCompact(catalog_wide, hint.auto_compact)) {
			AppendRewriteSkips(rows, hint, "auto_compact=false");
			continue;
		}
		if (hint.rewrite_steps.empty()) {
			AppendRewriteSkips(rows, hint, hint.rewrite_plan);
			continue;
		}
		for (const auto &step : hint.rewrite_steps) {
			if (!ShouldEmitRewriteCall(step.delete_threshold)) {
				rows.push_back(MakeRow("rewrite", hint.schema_name, hint.table_name, step.band, "skip",
				                       static_cast<int64_t>(step.files.size()), 0,
				                       policy::FormatRewriteStepDetails(step) + " | " +
				                           policy::kSkipNonPositiveRewriteThreshold));
				continue;
			}
			AppendPlanned(rows, "rewrite", step.band,
			              RewriteDataFilesCall(options.catalog, ref, step.delete_threshold), hint.schema_name,
			              hint.table_name, policy::FormatRewriteStepDetails(step),
			              static_cast<int64_t>(step.files.size()));
		}
	}

	for (const auto &hint : hints) {
		TableRef ref;
		ref.schema = hint.schema_name;
		ref.table = hint.table_name;
		std::vector<policy::FileStat> table_files;
		for (const auto &file : files) {
			if (file.schema_name == hint.schema_name && file.table_name == hint.table_name) {
				table_files.push_back(file);
			}
		}
		if (policy::SkipForAutoCompact(catalog_wide, hint.auto_compact)) {
			rows.push_back(MakeRow("merge", hint.schema_name, hint.table_name, "auto_compact", "skip",
			                       static_cast<int64_t>(hint.file_count), 0, "auto_compact=false"));
			continue;
		}
		for (const auto &tier : active.tiers) {
			uint64_t candidates = 0;
			for (const auto &file : table_files) {
				if (policy::FileQualifiesForMergeTier(file, tier)) {
					candidates++;
				}
			}
			if (candidates < 2) {
				rows.push_back(MakeRow("merge", hint.schema_name, hint.table_name, std::string("tier_") + tier.name,
				                       "skip", static_cast<int64_t>(candidates), 0, "need at least two files in band"));
				continue;
			}
			const int64_t cap =
			    options.max_compacted_files > 0 ? options.max_compacted_files : static_cast<int64_t>(tier.max_compacted_files);
			const std::string target = policy::EffectiveTargetFileSize(
			    policy::ResolveOption(catalog_options, "target_file_size", hint.schema_name, hint.table_name),
			    tier.target_file_size.c_str());
			const bool catalog_target = !hint.target_file_size.empty();
			if (!catalog_target) {
				std::ostringstream sql;
				sql << "CALL ducklake_set_option(" << QuoteString(options.catalog) << ", 'target_file_size', "
				    << QuoteString(target) << ")";
				AppendPlanned(rows, "merge", std::string("set_target_") + tier.name, sql.str(), hint.schema_name,
				              hint.table_name, sql.str());
			}
			std::ostringstream merge_sql;
			merge_sql << "CALL ducklake_merge_adjacent_files(" << QuoteString(options.catalog) << TableArg(ref)
			          << SchemaNamed(ref) << ", min_file_size => " << tier.min_file_size
			          << ", max_file_size => " << tier.max_file_size << ", max_compacted_files => " << cap << ")";
			AppendPlanned(rows, "merge", std::string("tier_") + tier.name, merge_sql.str(), hint.schema_name,
			              hint.table_name,
			              std::string("band ") + std::to_string(tier.min_file_size) + "-" +
			                  std::to_string(tier.max_file_size) + " -> " + target);
		}
	}

	return rows;
	} catch (const duckdb::InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const Exception &ex) {
		rows.push_back(MakeRow("maintain", options.table.schema, options.table.table, "orchestrate", "error", 0, 0,
		                       SafeWhat(ex)));
		return rows;
	} catch (const std::exception &ex) {
		rows.push_back(MakeRow("maintain", options.table.schema, options.table.table, "orchestrate", "error", 0, 0,
		                       SafeWhat(ex)));
		return rows;
	}
}

// Catalog-global retention only. Options load is best-effort: unread intervals
// fall back to named params (or skip expire / cleanup_all). Expire and cleanup
// are independent of each other and of table maintain. The session stays usable.
std::vector<MaintainRow> RunGlobalMaintain(duckdb::ClientContext &context, const GlobalMaintainOptions &options) {
	std::vector<MaintainRow> rows;
	std::string options_error;
	std::vector<policy::OptionBinding> catalog_options;
	try {
		catalog_options = LoadCatalogOptions(context, options.catalog, &options_error);
	} catch (const duckdb::InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const Exception &ex) {
		rows.push_back(MakeRow("catalog_options", "", "", "ducklake_options", "error", 0, 0, SafeWhat(ex)));
	} catch (const std::exception &ex) {
		rows.push_back(MakeRow("catalog_options", "", "", "ducklake_options", "error", 0, 0, SafeWhat(ex)));
	}

	try {
		if (!options_error.empty()) {
			rows.push_back(MakeRow("catalog_options", "", "", "ducklake_options", "error", 0, 0, options_error));
		}

		const std::string expire_interval = policy::EffectiveInterval(
		    options.expire_older_than_set, options.expire_older_than,
		    policy::ResolveOption(catalog_options, "expire_older_than", "", ""));
		const std::string delete_interval = policy::EffectiveInterval(
		    options.delete_older_than_set, options.delete_older_than,
		    policy::ResolveOption(catalog_options, "delete_older_than", "", ""));

		if (const char *skip = ExpireSkipReason(options.skip_expire, expire_interval.empty())) {
			rows.push_back(MakeRow("expire_snapshots", "", "", "expire", "skip", 0, 0, skip));
		} else {
			AppendPlanned(rows, "expire_snapshots", "expire", ExpireSnapshotsCall(options.catalog, expire_interval),
			              "", "", "older_than=" + expire_interval);
		}

		if (!ShouldRunCleanup(options.skip_cleanup)) {
			rows.push_back(MakeRow("cleanup_old_files", "", "", "cleanup", "skip", 0, 0, "skip_cleanup"));
		} else {
			AppendPlanned(rows, "cleanup_old_files", "cleanup", CleanupOldFilesCall(options.catalog, delete_interval),
			              "", "", delete_interval.empty() ? "cleanup_all" : "older_than=" + delete_interval);
			AppendPlanned(rows, "delete_orphaned_files", "cleanup", DeleteOrphanedFilesCall(options.catalog), "", "",
			              "remove unreferenced files");
		}

		return rows;
	} catch (const duckdb::InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const Exception &ex) {
		rows.push_back(MakeRow("maintain_global", "", "", "orchestrate", "error", 0, 0, SafeWhat(ex)));
		return rows;
	} catch (const std::exception &ex) {
		rows.push_back(MakeRow("maintain_global", "", "", "orchestrate", "error", 0, 0, SafeWhat(ex)));
		return rows;
	}
}

} // namespace lakemon
