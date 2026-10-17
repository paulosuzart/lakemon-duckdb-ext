#pragma once

#include "duckdb.hpp"
#include "lakemon_policy.hpp"

#include <string>
#include <vector>

namespace lakemon {

// Persist lakemon rewrite-ladder / merge-band overrides in the current DuckDB
// database (schema __lakemon). Rows are keyed by catalog name so each lake can
// have its own policy. This is not a DuckLake set_option key. Rewrite overrides
// store max_rewrite_steps (ladder size), min_delete_ratio, and optional
// byte_budget. Legacy high_min / medium_min / low_min columns are ignored.

void EnsurePolicyStore(duckdb::ClientContext &context);

std::vector<policy::StoredPolicyRow> LoadStoredPolicyRows(duckdb::ClientContext &context, const std::string &catalog,
                                                          std::string *error_out);

policy::ActivePolicy LoadActivePolicy(duckdb::ClientContext &context, const std::string &catalog,
                                      std::string *error_out);

void UpsertStoredPolicyRow(duckdb::ClientContext &context, const std::string &catalog,
                           const policy::StoredPolicyRow &row);

void DeleteStoredPolicyRow(duckdb::ClientContext &context, const std::string &catalog, const std::string &kind,
                           const std::string &name);

void DeleteStoredPolicyCatalog(duckdb::ClientContext &context, const std::string &catalog);

} // namespace lakemon
