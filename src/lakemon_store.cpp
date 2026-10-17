#include "lakemon_store.hpp"

#include "lakemon_compat.hpp"
#include "lakemon_sql.hpp"

#include "duckdb/common/exception.hpp"

#include <cstring>
#include <exception>
#include <new>
#include <sstream>
#include <string>

namespace lakemon {

using duckdb::InvalidInputException;

// Value columns after catalog. CREATE / SELECT / UPSERT all walk this table.
struct PolicyColumn {
	const char *name;
	const char *ddl_type;
};

static const PolicyColumn kPolicyValueCols[] = {
    {"kind", "VARCHAR NOT NULL"}, {"name", "VARCHAR NOT NULL"},    {"min_file_size", "BIGINT"},
    {"max_file_size", "BIGINT"},  {"target_file_size", "VARCHAR"}, {"max_compacted_files", "BIGINT"},
    {"high_min", "BIGINT"},       {"medium_min", "BIGINT"},        {"low_min", "BIGINT"},
    {"byte_budget", "BIGINT"},    {"max_rewrite_steps", "BIGINT"}, {"min_delete_ratio", "DOUBLE"}};

static const size_t kPolicyValueColCount = sizeof(kPolicyValueCols) / sizeof(kPolicyValueCols[0]);

static const char *const kSchemaSQL = "CREATE SCHEMA IF NOT EXISTS __lakemon";

static const char *const kAlterSQL[] = {
    "ALTER TABLE __lakemon.policy ADD COLUMN IF NOT EXISTS high_min BIGINT",
    "ALTER TABLE __lakemon.policy ADD COLUMN IF NOT EXISTS medium_min BIGINT",
    "ALTER TABLE __lakemon.policy ADD COLUMN IF NOT EXISTS low_min BIGINT",
    "ALTER TABLE __lakemon.policy ADD COLUMN IF NOT EXISTS byte_budget BIGINT",
    "ALTER TABLE __lakemon.policy ADD COLUMN IF NOT EXISTS max_rewrite_steps BIGINT",
    "ALTER TABLE __lakemon.policy ADD COLUMN IF NOT EXISTS min_delete_ratio DOUBLE"};

static std::string PolicySelectList() {
	std::ostringstream sql;
	for (size_t i = 0; i < kPolicyValueColCount; i++) {
		if (i > 0) {
			sql << ", ";
		}
		sql << kPolicyValueCols[i].name;
	}
	return sql.str();
}

static std::string PolicyCreateTableSQL() {
	std::ostringstream sql;
	sql << "CREATE TABLE IF NOT EXISTS __lakemon.policy (catalog VARCHAR NOT NULL";
	for (size_t i = 0; i < kPolicyValueColCount; i++) {
		sql << ", " << kPolicyValueCols[i].name << " " << kPolicyValueCols[i].ddl_type;
	}
	sql << ", PRIMARY KEY (catalog, kind, name))";
	return sql.str();
}

static duckdb::idx_t PolicyCol(const char *name) {
	for (duckdb::idx_t i = 0; i < static_cast<duckdb::idx_t>(kPolicyValueColCount); i++) {
		if (std::strcmp(kPolicyValueCols[i].name, name) == 0) {
			return i;
		}
	}
	return static_cast<duckdb::idx_t>(kPolicyValueColCount);
}

static std::string PolicyValueLiteral(const policy::StoredPolicyRow &row, const char *name) {
	if (std::strcmp(name, "kind") == 0) {
		return QuoteString(row.kind);
	}
	if (std::strcmp(name, "name") == 0) {
		return QuoteString(row.name);
	}
	if (std::strcmp(name, "min_file_size") == 0) {
		return std::to_string(row.min_file_size);
	}
	if (std::strcmp(name, "max_file_size") == 0) {
		return std::to_string(row.max_file_size);
	}
	if (std::strcmp(name, "target_file_size") == 0) {
		return QuoteString(row.target_file_size);
	}
	if (std::strcmp(name, "max_compacted_files") == 0) {
		return std::to_string(row.max_compacted_files);
	}
	if (std::strcmp(name, "high_min") == 0) {
		return std::to_string(row.high_min);
	}
	if (std::strcmp(name, "medium_min") == 0) {
		return std::to_string(row.medium_min);
	}
	if (std::strcmp(name, "low_min") == 0) {
		return std::to_string(row.low_min);
	}
	if (std::strcmp(name, "byte_budget") == 0) {
		return std::to_string(row.byte_budget);
	}
	if (std::strcmp(name, "max_rewrite_steps") == 0) {
		return std::to_string(row.max_rewrite_steps);
	}
	if (std::strcmp(name, "min_delete_ratio") == 0) {
		return std::to_string(row.min_delete_ratio);
	}
	throw InvalidInputException("lakemon: unknown policy column '%s'", name);
}

static std::string PolicyInsertValues(const std::string &catalog, const policy::StoredPolicyRow &row) {
	std::ostringstream sql;
	sql << QuoteString(catalog);
	for (size_t i = 0; i < kPolicyValueColCount; i++) {
		sql << ", " << PolicyValueLiteral(row, kPolicyValueCols[i].name);
	}
	return sql.str();
}

static bool IsSoftPolicyMigrateError(const std::string &msg) {
	return msg.find("does not exist") != std::string::npos || msg.find("already exists") != std::string::npos;
}

static void TryMigratePolicyStore(duckdb::ClientContext &context) {
	const size_t n = sizeof(kAlterSQL) / sizeof(kAlterSQL[0]);
	for (size_t i = 0; i < n; i++) {
		try {
			RunSQL(context, kAlterSQL[i]);
		} catch (const duckdb::InterruptException &) {
			throw;
		} catch (const std::bad_alloc &) {
			throw;
		} catch (const duckdb::Exception &ex) {
			const std::string msg = SafeWhat(ex);
			if (IsSoftPolicyMigrateError(msg)) {
				continue;
			}
			throw;
		}
	}
}

void EnsurePolicyStore(duckdb::ClientContext &context) {
	RunSQL(context, kSchemaSQL);
	RunSQL(context, PolicyCreateTableSQL());
	TryMigratePolicyStore(context);
}

static policy::StoredPolicyRow RowFromResult(QueryHandle &result, duckdb::idx_t i) {
	policy::StoredPolicyRow row;
	row.kind = CellString(result, PolicyCol("kind"), i);
	row.name = CellString(result, PolicyCol("name"), i);
	row.min_file_size = static_cast<uint64_t>(CellInt64(result, PolicyCol("min_file_size"), i));
	row.max_file_size = static_cast<uint64_t>(CellInt64(result, PolicyCol("max_file_size"), i));
	row.target_file_size = CellString(result, PolicyCol("target_file_size"), i);
	row.max_compacted_files = static_cast<uint64_t>(CellInt64(result, PolicyCol("max_compacted_files"), i));
	row.high_min = static_cast<uint64_t>(CellInt64(result, PolicyCol("high_min"), i));
	row.medium_min = static_cast<uint64_t>(CellInt64(result, PolicyCol("medium_min"), i));
	row.low_min = static_cast<uint64_t>(CellInt64(result, PolicyCol("low_min"), i));
	row.byte_budget = static_cast<uint64_t>(CellInt64(result, PolicyCol("byte_budget"), i));
	row.max_rewrite_steps = static_cast<uint64_t>(CellInt64(result, PolicyCol("max_rewrite_steps"), i));
	row.min_delete_ratio = CellDouble(result, PolicyCol("min_delete_ratio"), i);
	return row;
}

std::vector<policy::StoredPolicyRow> LoadStoredPolicyRows(duckdb::ClientContext &context, const std::string &catalog,
                                                          std::string *error_out) {
	std::vector<policy::StoredPolicyRow> rows;
	if (catalog.empty()) {
		if (error_out) {
			*error_out = "lakemon: catalog name is required";
		}
		return rows;
	}
	try {
		TryMigratePolicyStore(context);
		std::ostringstream sql;
		sql << "SELECT " << PolicySelectList() << " FROM __lakemon.policy WHERE catalog = " << QuoteString(catalog);
		auto result = RunSQL(context, sql.str());
		if (!result) {
			return rows;
		}
		const auto n = result->RowCount();
		for (duckdb::idx_t i = 0; i < n; i++) {
			policy::StoredPolicyRow row = RowFromResult(*result, i);
			if (row.kind.empty() || row.name.empty()) {
				continue;
			}
			rows.push_back(std::move(row));
		}
		if (error_out) {
			error_out->clear();
		}
		return rows;
	} catch (const duckdb::InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const duckdb::Exception &ex) {
		const std::string msg = SafeWhat(ex);
		// Missing schema/table means no overrides yet.
		if (msg.find("does not exist") != std::string::npos || msg.find("Catalog Error") != std::string::npos ||
		    msg.find("__lakemon") != std::string::npos) {
			if (error_out) {
				error_out->clear();
			}
			return rows;
		}
		if (error_out) {
			*error_out = msg;
		}
		return rows;
	} catch (const std::exception &ex) {
		if (error_out) {
			*error_out = SafeWhat(ex);
		}
		return rows;
	}
}

policy::ActivePolicy LoadActivePolicy(duckdb::ClientContext &context, const std::string &catalog,
                                      std::string *error_out) {
	std::string load_error;
	const std::vector<policy::StoredPolicyRow> rows = LoadStoredPolicyRows(context, catalog, &load_error);
	if (!load_error.empty()) {
		if (error_out) {
			*error_out = load_error;
		}
		return policy::DefaultPolicy();
	}
	std::string overlay_error;
	policy::ActivePolicy policy = policy::OverlayStoredRows(rows, &overlay_error);
	if (error_out) {
		*error_out = overlay_error;
	}
	return policy;
}

void UpsertStoredPolicyRow(duckdb::ClientContext &context, const std::string &catalog,
                           const policy::StoredPolicyRow &row) {
	if (catalog.empty() || row.kind.empty() || row.name.empty()) {
		throw InvalidInputException("lakemon: catalog, kind, and name are required");
	}
	EnsurePolicyStore(context);
	std::ostringstream sql;
	sql << "INSERT OR REPLACE INTO __lakemon.policy (catalog, " << PolicySelectList() << ") VALUES ("
	    << PolicyInsertValues(catalog, row) << ")";
	RunSQL(context, sql.str());
}

void DeleteStoredPolicyRow(duckdb::ClientContext &context, const std::string &catalog, const std::string &kind,
                           const std::string &name) {
	if (catalog.empty() || kind.empty() || name.empty()) {
		throw InvalidInputException("lakemon: catalog, kind, and name are required");
	}
	EnsurePolicyStore(context);
	std::ostringstream sql;
	sql << "DELETE FROM __lakemon.policy WHERE catalog = " << QuoteString(catalog)
	    << " AND kind = " << QuoteString(kind) << " AND name = " << QuoteString(name);
	RunSQL(context, sql.str());
}

void DeleteStoredPolicyCatalog(duckdb::ClientContext &context, const std::string &catalog) {
	if (catalog.empty()) {
		throw InvalidInputException("lakemon: catalog name is required");
	}
	EnsurePolicyStore(context);
	RunSQL(context, "DELETE FROM __lakemon.policy WHERE catalog = " + QuoteString(catalog));
}

} // namespace lakemon
