#pragma once

#include "duckdb.hpp"
#include "lakemon_diagnostics.hpp"
#include "lakemon_policy.hpp"
#include "lakemon_sql.hpp"

#include <string>
#include <vector>

namespace lakemon {

std::vector<policy::FileStat> InventoryFiles(duckdb::ClientContext &context, const std::string &catalog,
                                             const TableRef &filter,
                                             std::vector<InventoryDiagnostic> *diagnostics = nullptr);

// When diagnostics is set (maintain), unread rows are appended and hints are
// returned for tables that were read. When it is null (table_stats has no
// status column), a total unread inventory raises with every diagnostic joined
// so the failure is visible and not truncated to the first message.
std::vector<policy::TableHint> InventoryTables(duckdb::ClientContext &context, const std::string &catalog,
                                               const TableRef &filter,
                                               std::vector<InventoryDiagnostic> *diagnostics = nullptr,
                                               std::vector<policy::FileStat> *files_out = nullptr);

// Native DuckLake options via ducklake_options / catalog.options().
// On unread options, rows stay empty and error_out (if set) receives the message.
std::vector<policy::OptionBinding> LoadCatalogOptions(duckdb::ClientContext &context, const std::string &catalog,
                                                      std::string *error_out);

// Live DuckLake tables plus inlined-delete backlog (distinct file_ids still in
// ducklake_data_file, split active vs ended). Missing inlined-delete tables
// stay at zero. InventoryTables also uses this list so tables that only have
// inlined inserts (no parquet files yet) still get a flush_inlined row.
std::vector<policy::TableHint> LoadInlinedDeleteBacklog(duckdb::ClientContext &context, const std::string &catalog,
                                                        const TableRef &filter);

} // namespace lakemon
