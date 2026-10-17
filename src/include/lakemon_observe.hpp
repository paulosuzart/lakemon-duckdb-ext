// Maintain observability helpers (log lines, catalog-lock error hints).
// Header-only so policy-test can cover the public text without DuckDB.
#pragma once

#include <cstdint>
#include <sstream>
#include <string>

namespace lakemon {

static constexpr const char *kLogStderrSetting = "lakemon_log_stderr";
static constexpr const char *kLakemonLogTypeName = "lakemon";

static constexpr const char *kCatalogLockHint =
    "lakemon: DuckLake catalog commit could not obtain a Postgres lock "
    "(another DuckDB session may be holding the catalog). "
    "Commit or disconnect the other session, or put options=-c lock_timeout=30s "
    "on the Postgres ATTACH string so the wait fails fast.";

inline bool LooksLikeCatalogLockWait(const std::string &message) {
	if (message.find("lock timeout") != std::string::npos) {
		return true;
	}
	if (message.find("LockTimeout") != std::string::npos) {
		return true;
	}
	if (message.find("deadlock detected") != std::string::npos) {
		return true;
	}
	if (message.find("could not obtain lock") != std::string::npos) {
		return true;
	}
	if (message.find("canceling statement due to lock") != std::string::npos) {
		return true;
	}
	return false;
}

inline std::string AnnotateCatalogLockError(const std::string &message) {
	if (!LooksLikeCatalogLockWait(message)) {
		return message;
	}
	if (message.find("another DuckDB session") != std::string::npos) {
		return message;
	}
	if (message.empty()) {
		return kCatalogLockHint;
	}
	return message + " | " + kCatalogLockHint;
}

inline std::string QualifiedTable(const std::string &schema, const std::string &table) {
	if (schema.empty()) {
		return table;
	}
	if (table.empty()) {
		return schema;
	}
	return schema + "." + table;
}

// Stable stderr / log line. CI parses duration_ms and progress from stderr.
inline std::string FormatStepLog(bool start, const std::string &step, const std::string &action,
                                 const std::string &schema, const std::string &table, const std::string &status,
                                 int64_t files_processed, int64_t files_created, uint64_t planned_bytes,
                                 double progress, int64_t duration_ms, const std::string &error) {
	std::ostringstream out;
	out << "lakemon " << (start ? "start" : "end") << " step=" << step << " action=" << action
	    << " table=" << QualifiedTable(schema, table);
	if (!start) {
		out << " status=" << status << " duration_ms=" << duration_ms << " files_created=" << files_created;
		if (!error.empty()) {
			out << " error=" << error;
		}
	}
	out << " files_processed=" << files_processed << " planned_bytes=" << planned_bytes;
	out.setf(std::ios::fixed);
	out.precision(3);
	out << " progress=" << progress;
	return out.str();
}

} // namespace lakemon
