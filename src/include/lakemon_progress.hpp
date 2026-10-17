// Byte-weighted maintain progress. Header-only so the fraction can be tested
// without DuckDB. CALL / SELECT execute still runs in bind_replace, so the
// native table_scan_progress bar cannot advance during DuckLake work; logs
// and optional stderr report this same fraction after each step.
#pragma once

#include <cstddef>
#include <cstdint>

namespace lakemon {

// Weight of one result row in one unit: planned bytes when known, otherwise 1.
// File counts are not mixed into the byte total (they are a different unit).
inline uint64_t StepWeight(uint64_t planned_bytes, int64_t files_processed) {
	(void)files_processed;
	if (planned_bytes > 0) {
		return planned_bytes;
	}
	return 1;
}

inline uint64_t TotalStepWeight(const uint64_t *weights, size_t count) {
	uint64_t total = 0;
	for (size_t i = 0; i < count; i++) {
		total += weights[i];
	}
	return total;
}

// Completed / total in [0, 1]. Empty plan is 1 (nothing left to do).
inline double ProgressFraction(uint64_t completed_weight, uint64_t total_weight) {
	if (total_weight == 0) {
		return 1.0;
	}
	if (completed_weight >= total_weight) {
		return 1.0;
	}
	return static_cast<double>(completed_weight) / static_cast<double>(total_weight);
}

// DuckDB table_scan_progress reports percent of work (0–100).
inline double ProgressPercent(uint64_t completed_weight, uint64_t total_weight) {
	return ProgressFraction(completed_weight, total_weight) * 100.0;
}

} // namespace lakemon
