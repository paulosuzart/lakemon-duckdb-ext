// Resolve native DuckLake catalog options (table → schema → global).
// Header-only so precedence can be tested without DuckDB.
#pragma once

#include <cctype>
#include <cstdlib>
#include <string>
#include <vector>

namespace lakemon {
namespace policy {

struct OptionBinding {
	std::string name;
	std::string value;
	std::string scope;
	std::string scope_entry;
};

struct ResolvedOption {
	bool found;
	std::string value;
	std::string scope;

	ResolvedOption() : found(false) {
	}
};

inline char AsciiLower(char c) noexcept {
	if (c >= 'A' && c <= 'Z') {
		return static_cast<char>(c - 'A' + 'a');
	}
	return c;
}

inline bool EqualsCI(const std::string &a, const std::string &b) noexcept {
	if (a.size() != b.size()) {
		return false;
	}
	for (std::string::size_type i = 0; i < a.size(); i++) {
		if (AsciiLower(a[i]) != AsciiLower(b[i])) {
			return false;
		}
	}
	return true;
}

inline void SplitScopeEntry(const std::string &raw, std::string &schema, std::string &table) {
	schema.clear();
	table.clear();
	if (raw.empty()) {
		return;
	}
	const std::string::size_type dot = raw.find('.');
	if (dot == std::string::npos) {
		schema = "main";
		table = raw;
		return;
	}
	schema = raw.substr(0, dot);
	table = raw.substr(dot + 1);
}

// DuckLake precedence: table → schema → global (caller supplies built-in default).
inline ResolvedOption ResolveOption(const std::vector<OptionBinding> &rows, const std::string &name,
                                    const std::string &schema, const std::string &table) {
	ResolvedOption table_hit;
	ResolvedOption schema_hit;
	ResolvedOption global_hit;
	for (std::vector<OptionBinding>::size_type i = 0; i < rows.size(); i++) {
		const OptionBinding &row = rows[i];
		if (!EqualsCI(row.name, name)) {
			continue;
		}
		if (EqualsCI(row.scope, "TABLE")) {
			std::string entry_schema;
			std::string entry_table;
			SplitScopeEntry(row.scope_entry, entry_schema, entry_table);
			if (!table.empty() && EqualsCI(entry_schema, schema) && EqualsCI(entry_table, table)) {
				table_hit.found = true;
				table_hit.value = row.value;
				table_hit.scope = "TABLE";
			}
		} else if (EqualsCI(row.scope, "SCHEMA")) {
			if (!schema.empty() && EqualsCI(row.scope_entry, schema)) {
				schema_hit.found = true;
				schema_hit.value = row.value;
				schema_hit.scope = "SCHEMA";
			}
		} else if (row.scope.empty() || EqualsCI(row.scope, "GLOBAL")) {
			global_hit.found = true;
			global_hit.value = row.value;
			global_hit.scope = "GLOBAL";
		}
	}
	if (table_hit.found) {
		return table_hit;
	}
	if (schema_hit.found) {
		return schema_hit;
	}
	return global_hit;
}

inline bool TryParseDouble(const std::string &raw, double &out) noexcept {
	if (raw.empty()) {
		return false;
	}
	char *end = nullptr;
	const double value = std::strtod(raw.c_str(), &end);
	if (end == raw.c_str()) {
		return false;
	}
	while (end && *end && std::isspace(static_cast<unsigned char>(*end))) {
		end++;
	}
	if (end && *end != '\0') {
		return false;
	}
	out = value;
	return true;
}

inline bool TryParseBool(const std::string &raw, bool &out) noexcept {
	if (EqualsCI(raw, "true") || EqualsCI(raw, "1") || EqualsCI(raw, "yes")) {
		out = true;
		return true;
	}
	if (EqualsCI(raw, "false") || EqualsCI(raw, "0") || EqualsCI(raw, "no")) {
		out = false;
		return true;
	}
	return false;
}

// Catalog rewrite_delete_threshold, when set in (0, 1], is the value passed to
// ducklake_rewrite_data_files (operator configured once). ApplyNativeOptions
// then collapses planned rungs to a single CALL. 0 / negative is not a lakemon
// CALL threshold (full-table rewrite); ApplyNativeOptions skips rewrite instead.
inline double EffectiveRewriteThreshold(double ladder_threshold, const ResolvedOption &catalog) noexcept {
	double catalog_value = 0;
	if (catalog.found && TryParseDouble(catalog.value, catalog_value) && catalog_value > 0.0 && catalog_value <= 1.0) {
		return catalog_value;
	}
	return ladder_threshold;
}

// CALL named param wins; otherwise the resolved catalog option (usually GLOBAL).
inline std::string EffectiveInterval(bool call_set, const std::string &call_value, const ResolvedOption &catalog) {
	if (call_set) {
		return call_value;
	}
	if (catalog.found) {
		return catalog.value;
	}
	return std::string();
}

inline bool EffectiveAutoCompact(const ResolvedOption &catalog) noexcept {
	bool value = true;
	if (catalog.found && TryParseBool(catalog.value, value)) {
		return value;
	}
	return true;
}

// Catalog-wide maintain skips tables with auto_compact=false. An explicit table
// argument still maintains that table.
inline bool SkipForAutoCompact(bool catalog_wide, bool auto_compact) noexcept {
	return catalog_wide && !auto_compact;
}

inline std::string EffectiveTargetFileSize(const ResolvedOption &catalog, const char *band_default) {
	if (catalog.found && !catalog.value.empty()) {
		return catalog.value;
	}
	return band_default ? std::string(band_default) : std::string();
}

} // namespace policy
} // namespace lakemon
