// Maintain pipeline rules. Header-only so the contract can be tested without DuckDB.
#pragma once

#include <string>

namespace lakemon {

// After inventory succeeds, flush / rewrite / merge are planned independently.
// Only a hard inventory failure stops the table pipeline before rewrite/merge.
// PRAGMA execute is fail-fast (a failed statement stops the rest of the batch).
// Expire, old-file cleanup, and orphan delete are catalog-global and live in
// lakemon_maintain_global, not lakemon_maintain.
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

// Table-function CALL / SELECT only plans. Mutations run as PRAGMA replacement
// SQL (the official way to execute statements outside the outer TF transaction).
inline bool TableFunctionRunsDuckLakeCalls() {
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
