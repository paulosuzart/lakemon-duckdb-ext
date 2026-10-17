#pragma once

#include <cstdint>
#include <sstream>
#include <string>

namespace lakemon {

inline std::string QuoteIdent(const std::string &ident) {
	std::string out = "\"";
	for (char c : ident) {
		if (c == '"') {
			out += "\"\"";
		} else {
			out += c;
		}
	}
	out += "\"";
	return out;
}

inline std::string QuoteString(const std::string &value) {
	std::string out = "'";
	for (char c : value) {
		if (c == '\'') {
			out += "''";
		} else {
			out += c;
		}
	}
	out += "'";
	return out;
}

inline std::string MetadataCatalog(const std::string &catalog) {
	return "__ducklake_metadata_" + catalog;
}

struct TableRef {
	std::string schema;
	std::string table;

	TableRef() : schema("main") {
	}
};

inline TableRef ParseTableRef(const std::string &raw) {
	TableRef ref;
	if (raw.empty()) {
		return ref;
	}
	const std::string::size_type dot = raw.find('.');
	if (dot == std::string::npos) {
		ref.table = raw;
		return ref;
	}
	ref.schema = raw.substr(0, dot);
	ref.table = raw.substr(dot + 1);
	return ref;
}

// DuckLake CALL fragments. Flush takes named table_name / schema_name only.
// rewrite, merge, and list_files take a positional table and schema =>.
inline std::string TableArg(const TableRef &ref, const char *named = nullptr) {
	if (ref.table.empty()) {
		return "";
	}
	if (named && named[0] != '\0') {
		return std::string(", ") + named + " => " + QuoteString(ref.table);
	}
	return ", " + QuoteString(ref.table);
}

inline std::string SchemaNamed(const TableRef &ref, const char *param = "schema") {
	if (ref.table.empty() || ref.schema.empty() || ref.schema == "main") {
		return "";
	}
	return std::string(", ") + param + " => " + QuoteString(ref.schema);
}

inline std::string InlinedDeleteTableName(int64_t table_id) {
	return "ducklake_inlined_delete_" + std::to_string(table_id);
}

inline std::string FlushInlinedDataCall(const std::string &catalog, const TableRef &ref) {
	return "CALL ducklake_flush_inlined_data(" + QuoteString(catalog) + TableArg(ref, "table_name") +
	       SchemaNamed(ref, "schema_name") + ")";
}

inline std::string RewriteDataFilesCall(const std::string &catalog, const TableRef &ref, double delete_threshold) {
	std::ostringstream sql;
	sql << "CALL ducklake_rewrite_data_files(" << QuoteString(catalog) << TableArg(ref) << SchemaNamed(ref)
	    << ", delete_threshold => " << delete_threshold << ")";
	return sql.str();
}

// Cast now() to TIMESTAMP so older_than binds without ICU (TIMESTAMPTZ interval
// arithmetic is what requires the icu extension).
inline std::string TimestampMinusInterval(const std::string &older_than) {
	return "now()::TIMESTAMP - INTERVAL " + QuoteString(older_than);
}

inline std::string ExpireSnapshotsCall(const std::string &catalog, const std::string &older_than) {
	return "CALL ducklake_expire_snapshots(" + QuoteString(catalog) + ", older_than => " +
	       TimestampMinusInterval(older_than) + ")";
}

inline std::string CleanupOldFilesCall(const std::string &catalog, const std::string &older_than) {
	std::ostringstream sql;
	sql << "CALL ducklake_cleanup_old_files(" << QuoteString(catalog);
	if (!older_than.empty()) {
		sql << ", older_than => " << TimestampMinusInterval(older_than);
	} else {
		sql << ", cleanup_all => true";
	}
	sql << ")";
	return sql.str();
}

inline std::string DeleteOrphanedFilesCall(const std::string &catalog) {
	return "CALL ducklake_delete_orphaned_files(" + QuoteString(catalog) + ")";
}

} // namespace lakemon
