// Maintain pipeline rules. Header-only so the contract can be tested without DuckDB.
#pragma once

#include <cctype>
#include <string>

namespace lakemon {

// After inventory succeeds, flush / rewrite / merge are planned independently.
// Only a hard inventory failure stops the table pipeline before rewrite/merge.
// Execute (CALL / SELECT / PRAGMA) is fail-fast: a failed DuckLake step stops
// the rest of the batch. Expire, old-file cleanup, and orphan delete are
// catalog-global and live in lakemon_maintain_global, not lakemon_maintain.
inline bool InventoryFailureStopsPipeline() {
	return true;
}

inline bool FlushErrorSkipsStep(const std::string &step) {
	(void)step;
	return false;
}

inline bool TableMaintainIncludesRetention() {
	return false;
}

inline bool GlobalMaintainIncludesTableWork() {
	return false;
}

// CALL / SELECT bind_replace plans, then ExecutePlannedRows runs each DuckLake
// CALL on a nested Connection (own commit). DuckLake forbids mixing data
// changes and compaction in one transaction, so one-query CTE execute cannot
// flush + set_option + merge together. Not table-function init (that deadlocks).
// Moving execute into table-function GetData would be needed for a live
// progress bar and streamed result rows; that is not done (same catalog lock
// as init). Logs report byte-weighted progress after each step instead.
// dry_run => false is allowed only for CALL / SELECT / FROM / WITH / PRAGMA in
// autocommit. EXPLAIN, PREPARE, CREATE VIEW, and other wrappers are refused.
inline bool TableFunctionRunsDuckLakeCalls() {
	return true;
}

// First SQL keyword after whitespace, opening parens, and comments.
inline std::string LeadingSQLKeyword(const std::string &query) {
	size_t i = 0;
	const size_t n = query.size();
	while (i < n) {
		const unsigned char c = static_cast<unsigned char>(query[i]);
		if (std::isspace(c) || query[i] == '(') {
			i++;
			continue;
		}
		if (query[i] == '-' && i + 1 < n && query[i + 1] == '-') {
			i += 2;
			while (i < n && query[i] != '\n') {
				i++;
			}
			continue;
		}
		if (query[i] == '/' && i + 1 < n && query[i + 1] == '*') {
			i += 2;
			while (i + 1 < n && !(query[i] == '*' && query[i + 1] == '/')) {
				i++;
			}
			if (i + 1 < n) {
				i += 2;
			}
			continue;
		}
		break;
	}
	const size_t start = i;
	while (i < n) {
		const unsigned char c = static_cast<unsigned char>(query[i]);
		if (std::isalpha(c) || c == '_') {
			i++;
		} else {
			break;
		}
	}
	std::string keyword = query.substr(start, i - start);
	for (size_t k = 0; k < keyword.size(); k++) {
		if (keyword[k] >= 'a' && keyword[k] <= 'z') {
			keyword[k] = static_cast<char>(keyword[k] - 'a' + 'A');
		}
	}
	return keyword;
}

// Statements that bind and execute as one autocommit query. Deny everything else
// (EXPLAIN, PREPARE, EXECUTE, DESCRIBE, SUMMARIZE, CREATE VIEW, INSERT, COPY).
inline bool IsMaintainExecuteKeyword(const std::string &keyword) {
	return keyword == "CALL" || keyword == "SELECT" || keyword == "FROM" || keyword == "WITH" || keyword == "PRAGMA";
}

// True when dry_run => false must be refused. error is a complete lakemon: message.
// has_prepared_parameters is structural (Binder parameter map), not query text.
inline bool MaintainExecuteRefused(bool auto_commit, const std::string &query, std::string &error,
                                   bool has_prepared_parameters = false) {
	if (has_prepared_parameters) {
		error = "lakemon: dry_run => false cannot be prepared with parameters "
		        "(C API / driver prepare). Use a plain CALL or SELECT with literals, "
		        "or dry_run => true.";
		return true;
	}
	if (!auto_commit) {
		error = "lakemon: dry_run => false is not allowed inside an explicit transaction "
		        "(BEGIN). Commit first, or use dry_run => true. Nested execute would wait "
		        "on the catalog lock held by this transaction.";
		return true;
	}
	const std::string keyword = LeadingSQLKeyword(query);
	if (!IsMaintainExecuteKeyword(keyword)) {
		error = "lakemon: dry_run => false requires a plain CALL, SELECT, FROM, WITH, or "
		        "PRAGMA in autocommit (got " +
		        (keyword.empty() ? "empty" : keyword) +
		        "). EXPLAIN, PREPARE, CREATE VIEW, and other wrappers bind without a "
		        "single execution, or rebind.";
		return true;
	}
	return false;
}

// Prior skip semantics, now on CALL lakemon_maintain_global.
// nullptr means run expire; otherwise the skip details string.
inline const char *ExpireSkipReason(bool skip_expire, bool interval_empty) {
	if (skip_expire) {
		return "skip_expire";
	}
	if (interval_empty) {
		return "expire_older_than not set";
	}
	return nullptr;
}

inline bool ShouldRunCleanup(bool skip_cleanup) {
	return !skip_cleanup;
}

// ducklake_rewrite_data_files with delete_threshold 0 matches every file.
// lakemon never plans or executes that CALL.
inline bool ShouldEmitRewriteCall(double delete_threshold) {
	return delete_threshold > 0.0;
}

} // namespace lakemon
