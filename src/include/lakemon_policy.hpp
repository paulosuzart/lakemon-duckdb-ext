// Standalone policy for DuckLake maintain orchestration.
// Header-only so the byte-weighted rewrite ladder and merge tiers can be tested
// without DuckDB.
#pragma once

#include "lakemon_options.hpp"

#include <algorithm>
#include <cstdint>
#include <sstream>
#include <string>
#include <vector>

namespace lakemon {
namespace policy {

constexpr uint64_t kMiB = 1024ULL * 1024ULL;

static constexpr const char *kKindRewriteLadder = "rewrite_ladder";
static constexpr const char *kKindMergeTier = "merge_tier";
static constexpr const char *kKindSetting = "setting";
static constexpr const char *kLadderName = "default";
static constexpr const char *kFlushInlinedMaxDeleteFilesName = "flush_inlined_max_delete_files";
static constexpr const char *kSourceDefault = "default";
static constexpr const char *kSourceOverride = "override";

static constexpr uint64_t kDefaultMaxRewriteSteps = 3;
static constexpr uint64_t kMaxRewriteStepsLimit = 16;
static constexpr double kDefaultMinDeleteRatio = 0.01;

struct FileStat {
	std::string schema_name;
	std::string table_name;
	uint64_t data_file_id = 0;
	uint64_t file_size_bytes = 0;
	uint64_t record_count = 0;
	uint64_t delete_count = 0;
	uint64_t delete_file_size_bytes = 0;
};

// Byte-weighted delete_threshold ladder. N = max_rewrite_steps cuts when
// cumulative file_size_bytes crosses total_bytes * i/N. Thresholds are the
// delete fraction of the file that crosses each cut (worst-first). The last
// cut is the smallest observed fraction among files that meet min_delete_ratio
// — never 0.0. Files below min_delete_ratio are excluded from the byte pool.
struct RewriteLadder {
	// 0 = unset. Optional per-rung size cap after cuts are chosen.
	uint64_t byte_budget = 0;
	uint64_t max_rewrite_steps = kDefaultMaxRewriteSteps;
	double min_delete_ratio = kDefaultMinDeleteRatio;
};

struct MergeTier {
	std::string name;
	uint64_t min_file_size;
	uint64_t max_file_size;
	std::string target_file_size;
	uint64_t max_compacted_files;
};

// One persisted override row. A write stores the full effective ladder or tier.
// high_min / medium_min / low_min are leftover store columns (ignored).
struct StoredPolicyRow {
	std::string kind;
	std::string name;
	uint64_t min_file_size = 0;
	uint64_t max_file_size = 0;
	std::string target_file_size;
	uint64_t max_compacted_files = 0;
	uint64_t high_min = 0;
	uint64_t medium_min = 0;
	uint64_t low_min = 0;
	uint64_t byte_budget = 0;
	uint64_t max_rewrite_steps = 0;
	double min_delete_ratio = 0;
};

// Sparse field patch from CALL lakemon_set_policy named parameters.
struct PolicyFieldPatch {
	bool set_byte_budget = false;
	uint64_t byte_budget = 0;
	bool set_max_rewrite_steps = false;
	uint64_t max_rewrite_steps = 0;
	bool set_min_delete_ratio = false;
	double min_delete_ratio = 0;
	bool set_min_file_size = false;
	uint64_t min_file_size = 0;
	bool set_max_file_size = false;
	uint64_t max_file_size = 0;
	bool set_target_file_size = false;
	std::string target_file_size;
	bool set_max_compacted_files = false;
	uint64_t max_compacted_files = 0;
	bool reset = false;
};

struct ActivePolicy {
	RewriteLadder rewrite;
	std::vector<MergeTier> tiers;
	std::vector<std::string> overridden_keys;
};

// One rewrite CALL: files newly covered by a byte-weighted threshold cut.
struct PlannedRewriteStep {
	std::string band;
	std::vector<FileStat> files;
	double delete_threshold = 0.0;
	uint64_t planned_bytes = 0;
	uint64_t planned_deletes = 0;
};

// Byte-weighted deleted payload for table stats (not a bucket key).
inline uint64_t DeletedBytes(const FileStat &file) {
	if (file.record_count > 0 && file.delete_count > 0) {
		const double ratio =
		    std::min(1.0, static_cast<double>(file.delete_count) / static_cast<double>(file.record_count));
		return static_cast<uint64_t>(ratio * static_cast<double>(file.file_size_bytes));
	}
	return file.delete_file_size_bytes;
}

inline double DeleteRatio(const FileStat &file) {
	if (file.record_count > 0) {
		return std::min(1.0, static_cast<double>(file.delete_count) / static_cast<double>(file.record_count));
	}
	// record_count unknown (ducklake_list_files fallback): conservative size ratio.
	if (file.file_size_bytes > 0 && file.delete_file_size_bytes > 0) {
		return std::min(1.0,
		                static_cast<double>(file.delete_file_size_bytes) / static_cast<double>(file.file_size_bytes));
	}
	if (file.delete_file_size_bytes > 0 || file.delete_count > 0) {
		return 1.0;
	}
	return 0.0;
}

inline bool HasPositiveDeleteFraction(const FileStat &file) {
	return DeleteRatio(file) > 0.0;
}

inline bool QualifiesForRewrite(const FileStat &file, double min_delete_ratio) {
	const double ratio = DeleteRatio(file);
	return ratio > 0.0 && ratio >= min_delete_ratio;
}

inline std::string RewriteRungName(std::size_t index_1based) {
	return "rung_" + std::to_string(index_1based);
}

inline uint64_t SaturatingAdd(uint64_t a, uint64_t b) {
	if (b > ~static_cast<uint64_t>(0) - a) {
		return ~static_cast<uint64_t>(0);
	}
	return a + b;
}

// Portable unsigned 128-bit product. Avoids unsigned __int128 (MSVC C4235).
inline void UMul128(uint64_t x, uint64_t y, uint64_t &hi, uint64_t &lo) {
	const uint64_t x0 = x & 0xffffffffULL;
	const uint64_t x1 = x >> 32;
	const uint64_t y0 = y & 0xffffffffULL;
	const uint64_t y1 = y >> 32;
	const uint64_t p00 = x0 * y0;
	const uint64_t p01 = x0 * y1;
	const uint64_t p10 = x1 * y0;
	const uint64_t p11 = x1 * y1;
	const uint64_t mid = (p00 >> 32) + (p01 & 0xffffffffULL) + (p10 & 0xffffffffULL);
	lo = (p00 & 0xffffffffULL) | (mid << 32);
	hi = p11 + (p01 >> 32) + (p10 >> 32) + (mid >> 32);
}

// a * b >= c * d without wrapping uint64_t products.
inline bool ProductGreaterEqual(uint64_t a, uint64_t b, uint64_t c, uint64_t d) {
	uint64_t a_hi = 0;
	uint64_t a_lo = 0;
	uint64_t c_hi = 0;
	uint64_t c_lo = 0;
	UMul128(a, b, a_hi, a_lo);
	UMul128(c, d, c_hi, c_lo);
	if (a_hi != c_hi) {
		return a_hi > c_hi;
	}
	return a_lo >= c_lo;
}

// cum / total >= i / n, overflow-safe on MSVC and 64-bit hosts.
inline bool CumulativeCrossesCut(uint64_t cumulative, uint64_t total, uint64_t i, uint64_t n) {
	if (n == 0 || i == 0 || total == 0) {
		return false;
	}
	return ProductGreaterEqual(cumulative, n, total, i);
}

inline bool SameThreshold(double a, double b) noexcept {
	const double diff = a > b ? a - b : b - a;
	return diff <= 1e-15;
}

inline bool ValidateRewriteLadder(const RewriteLadder &ladder, std::string &error) {
	if (ladder.max_rewrite_steps < 1 || ladder.max_rewrite_steps > kMaxRewriteStepsLimit) {
		error = "lakemon: max_rewrite_steps must be between 1 and 16";
		return false;
	}
	if (!(ladder.min_delete_ratio > 0.0 && ladder.min_delete_ratio <= 1.0)) {
		error = "lakemon: min_delete_ratio must be in (0, 1]";
		return false;
	}
	return true;
}

inline RewriteLadder DefaultRewriteLadder() {
	return RewriteLadder();
}

// Merge size bands inspired by common DuckLake maintain patterns (streaming
// compaction: micro -> small -> medium).
inline const std::vector<MergeTier> &DefaultMergeTiers() {
	static const std::vector<MergeTier> kTiers = {
	    {"micro", 0, 1 * kMiB, "5MB", 64},
	    {"small", 1 * kMiB, 10 * kMiB, "32MB", 32},
	    {"medium", 10 * kMiB, 64 * kMiB, "128MB", 16},
	};
	return kTiers;
}

inline ActivePolicy DefaultPolicy() {
	ActivePolicy policy;
	policy.rewrite = DefaultRewriteLadder();
	policy.tiers = DefaultMergeTiers();
	return policy;
}

inline std::string PolicyOverrideKey(const std::string &kind, const std::string &name) {
	return kind + ":" + name;
}

inline bool PolicyKeyOverridden(const ActivePolicy &policy, const std::string &kind, const std::string &name) {
	const std::string key = PolicyOverrideKey(kind, name);
	for (const auto &entry : policy.overridden_keys) {
		if (entry == key) {
			return true;
		}
	}
	return false;
}

inline void MarkOverridden(ActivePolicy &policy, const std::string &kind, const std::string &name) {
	if (PolicyKeyOverridden(policy, kind, name)) {
		return;
	}
	policy.overridden_keys.push_back(PolicyOverrideKey(kind, name));
}

inline void ClearOverridden(ActivePolicy &policy, const std::string &kind, const std::string &name) {
	const std::string key = PolicyOverrideKey(kind, name);
	std::vector<std::string> kept;
	for (const auto &entry : policy.overridden_keys) {
		if (entry != key) {
			kept.push_back(entry);
		}
	}
	policy.overridden_keys.swap(kept);
}

inline bool InUnitInterval(double value) noexcept {
	return value >= 0.0 && value <= 1.0;
}

// Native ducklake_rewrite_data_files delete_threshold must be in (0, 1].
// 0.0 matches every file (including those with no deletes).
inline bool PositiveRewriteThreshold(double value) noexcept {
	return value > 0.0 && value <= 1.0;
}

static constexpr const char *kSkipNonPositiveRewriteThreshold =
    "rewrite_delete_threshold<=0; use native CALL ducklake_rewrite_data_files(..., delete_threshold => 0)";
static constexpr const char *kSkipCatalogThresholdMiss = "no files meet catalog rewrite_delete_threshold";

inline bool ValidateMergeTier(const MergeTier &tier, std::string &error) {
	if (tier.name.empty()) {
		error = "lakemon: merge tier name is required";
		return false;
	}
	if (tier.min_file_size >= tier.max_file_size) {
		error = "lakemon: min_file_size must be < max_file_size";
		return false;
	}
	if (tier.target_file_size.empty()) {
		error = "lakemon: target_file_size is required";
		return false;
	}
	if (tier.max_compacted_files < 1) {
		error = "lakemon: max_compacted_files must be >= 1";
		return false;
	}
	return true;
}

inline MergeTier *FindMergeTier(ActivePolicy &policy, const std::string &name) {
	for (auto &tier : policy.tiers) {
		if (EqualsCI(tier.name, name)) {
			return &tier;
		}
	}
	return nullptr;
}

inline const MergeTier *FindMergeTier(const ActivePolicy &policy, const std::string &name) {
	for (const auto &tier : policy.tiers) {
		if (EqualsCI(tier.name, name)) {
			return &tier;
		}
	}
	return nullptr;
}

inline bool KnownPolicyKind(const std::string &kind) noexcept {
	return EqualsCI(kind, kKindRewriteLadder) || EqualsCI(kind, kKindMergeTier);
}

inline bool NormalizePolicyKind(const std::string &kind, std::string &canonical, std::string &error) {
	if (EqualsCI(kind, kKindRewriteLadder)) {
		canonical = kKindRewriteLadder;
		return true;
	}
	if (EqualsCI(kind, kKindMergeTier)) {
		canonical = kKindMergeTier;
		return true;
	}
	error = "lakemon: kind must be rewrite_ladder or merge_tier";
	return false;
}

inline bool IsRewriteLadderName(const std::string &name) noexcept {
	return EqualsCI(name, kLadderName);
}

inline bool PatchHasField(const PolicyFieldPatch &patch) noexcept {
	return patch.set_byte_budget || patch.set_max_rewrite_steps || patch.set_min_delete_ratio ||
	       patch.set_min_file_size || patch.set_max_file_size || patch.set_target_file_size ||
	       patch.set_max_compacted_files;
}

inline bool PatchHasRewriteField(const PolicyFieldPatch &patch) noexcept {
	return patch.set_byte_budget || patch.set_max_rewrite_steps || patch.set_min_delete_ratio;
}

inline bool PatchHasMergeField(const PolicyFieldPatch &patch) noexcept {
	return patch.set_min_file_size || patch.set_max_file_size || patch.set_target_file_size ||
	       patch.set_max_compacted_files;
}

inline bool ResetNamedPolicy(ActivePolicy &policy, const std::string &kind, const std::string &name,
                             std::string &error) {
	std::string canonical;
	if (!NormalizePolicyKind(kind, canonical, error)) {
		return false;
	}
	if (canonical == kKindRewriteLadder) {
		if (!IsRewriteLadderName(name)) {
			error = "lakemon: unknown rewrite ladder '" + name + "'";
			return false;
		}
		policy.rewrite = DefaultRewriteLadder();
		ClearOverridden(policy, canonical, kLadderName);
		return true;
	}
	const MergeTier *def = nullptr;
	for (const auto &tier : DefaultMergeTiers()) {
		if (EqualsCI(tier.name, name)) {
			def = &tier;
			break;
		}
	}
	MergeTier *cur = FindMergeTier(policy, name);
	if (!def || !cur) {
		error = "lakemon: unknown merge tier '" + name + "'";
		return false;
	}
	*cur = *def;
	ClearOverridden(policy, canonical, cur->name);
	return true;
}

// Apply a sparse CALL patch onto the rewrite ladder or a named merge tier.
// Rewrite name must be "default". Merge names stay micro/small/medium.
inline bool ApplyPolicyPatch(ActivePolicy &policy, const std::string &kind, const std::string &name,
                             const PolicyFieldPatch &patch, std::string &error) {
	std::string canonical;
	if (!NormalizePolicyKind(kind, canonical, error)) {
		return false;
	}
	if (name.empty()) {
		error = "lakemon: policy name is required";
		return false;
	}
	if (patch.reset) {
		return ResetNamedPolicy(policy, canonical, name, error);
	}
	if (!PatchHasField(patch)) {
		error = "lakemon: set_policy requires at least one field or reset => true";
		return false;
	}
	if (canonical == kKindRewriteLadder) {
		if (!IsRewriteLadderName(name)) {
			error = "lakemon: unknown rewrite ladder '" + name + "'";
			return false;
		}
		if (PatchHasMergeField(patch)) {
			error = "lakemon: merge-tier fields are not valid for rewrite_ladder";
			return false;
		}
		RewriteLadder next = policy.rewrite;
		if (patch.set_byte_budget) {
			next.byte_budget = patch.byte_budget;
		}
		if (patch.set_max_rewrite_steps) {
			next.max_rewrite_steps = patch.max_rewrite_steps;
		}
		if (patch.set_min_delete_ratio) {
			next.min_delete_ratio = patch.min_delete_ratio;
		}
		if (!ValidateRewriteLadder(next, error)) {
			return false;
		}
		policy.rewrite = next;
		MarkOverridden(policy, canonical, kLadderName);
		return true;
	}
	MergeTier *tier = FindMergeTier(policy, name);
	if (!tier) {
		error = "lakemon: unknown merge tier '" + name + "'";
		return false;
	}
	if (PatchHasRewriteField(patch)) {
		error = "lakemon: rewrite-ladder fields are not valid for merge_tier";
		return false;
	}
	MergeTier next = *tier;
	if (patch.set_min_file_size) {
		next.min_file_size = patch.min_file_size;
	}
	if (patch.set_max_file_size) {
		next.max_file_size = patch.max_file_size;
	}
	if (patch.set_target_file_size) {
		next.target_file_size = patch.target_file_size;
	}
	if (patch.set_max_compacted_files) {
		next.max_compacted_files = patch.max_compacted_files;
	}
	if (!ValidateMergeTier(next, error)) {
		return false;
	}
	*tier = next;
	MarkOverridden(policy, canonical, tier->name);
	return true;
}

inline StoredPolicyRow RowFromLadder(const RewriteLadder &ladder) {
	StoredPolicyRow row;
	row.kind = kKindRewriteLadder;
	row.name = kLadderName;
	row.byte_budget = ladder.byte_budget;
	row.max_rewrite_steps = ladder.max_rewrite_steps;
	row.min_delete_ratio = ladder.min_delete_ratio;
	return row;
}

inline StoredPolicyRow RowFromTier(const MergeTier &tier) {
	StoredPolicyRow row;
	row.kind = kKindMergeTier;
	row.name = tier.name;
	row.min_file_size = tier.min_file_size;
	row.max_file_size = tier.max_file_size;
	row.target_file_size = tier.target_file_size;
	row.max_compacted_files = tier.max_compacted_files;
	return row;
}

inline bool OverlayStoredRow(ActivePolicy &policy, const StoredPolicyRow &row, std::string &error) {
	std::string canonical;
	if (!NormalizePolicyKind(row.kind, canonical, error)) {
		return false;
	}
	if (canonical == kKindRewriteLadder) {
		if (!IsRewriteLadderName(row.name)) {
			error = "lakemon: unknown rewrite ladder '" + row.name + "'";
			return false;
		}
		RewriteLadder next;
		next.byte_budget = row.byte_budget;
		next.max_rewrite_steps = row.max_rewrite_steps == 0 ? kDefaultMaxRewriteSteps : row.max_rewrite_steps;
		next.min_delete_ratio =
		    (row.min_delete_ratio > 0.0 && row.min_delete_ratio <= 1.0) ? row.min_delete_ratio : kDefaultMinDeleteRatio;
		if (!ValidateRewriteLadder(next, error)) {
			return false;
		}
		policy.rewrite = next;
		MarkOverridden(policy, canonical, kLadderName);
		return true;
	}
	MergeTier *tier = FindMergeTier(policy, row.name);
	if (!tier) {
		error = "lakemon: unknown merge tier '" + row.name + "'";
		return false;
	}
	MergeTier next = *tier;
	next.min_file_size = row.min_file_size;
	next.max_file_size = row.max_file_size;
	next.target_file_size = row.target_file_size;
	next.max_compacted_files = row.max_compacted_files;
	if (!ValidateMergeTier(next, error)) {
		return false;
	}
	*tier = next;
	MarkOverridden(policy, canonical, tier->name);
	return true;
}

// Built-in defaults overlaid with persisted rows. Invalid rows are skipped
// (first error is recorded) so a bad store cannot abort classification.
inline ActivePolicy OverlayStoredRows(const std::vector<StoredPolicyRow> &rows, std::string *error_out = nullptr) {
	ActivePolicy policy = DefaultPolicy();
	for (const auto &row : rows) {
		std::string error;
		if (!OverlayStoredRow(policy, row, error)) {
			if (error_out && error_out->empty()) {
				*error_out = error;
			}
		}
	}
	return policy;
}

inline std::vector<FileStat> TakeWithinBudget(std::vector<FileStat> files, uint64_t byte_budget) {
	if (byte_budget == 0) {
		return files;
	}
	std::vector<FileStat> chosen;
	uint64_t used = 0;
	for (auto &file : files) {
		uint64_t next = used;
		if (file.file_size_bytes > ~static_cast<uint64_t>(0) - used) {
			next = ~static_cast<uint64_t>(0);
		} else {
			next = used + file.file_size_bytes;
		}
		if (chosen.empty() || next <= byte_budget) {
			used = next;
			chosen.push_back(std::move(file));
		}
	}
	return chosen;
}

inline bool SortDirtyWorstFirst(const FileStat &a, const FileStat &b) {
	const double ra = DeleteRatio(a);
	const double rb = DeleteRatio(b);
	if (ra != rb) {
		return ra > rb;
	}
	if (a.file_size_bytes != b.file_size_bytes) {
		return a.file_size_bytes > b.file_size_bytes;
	}
	return a.data_file_id < b.data_file_id;
}

inline void RefreshRewriteStepTotals(PlannedRewriteStep &step) {
	step.planned_bytes = 0;
	step.planned_deletes = 0;
	for (const auto &file : step.files) {
		step.planned_bytes += file.file_size_bytes;
		step.planned_deletes += file.delete_count;
	}
}

inline bool RungHasFileId(const PlannedRewriteStep &step, uint64_t data_file_id) {
	for (const auto &file : step.files) {
		if (file.data_file_id == data_file_id) {
			return true;
		}
	}
	return false;
}

inline void MergeRewriteRung(PlannedRewriteStep &dst, PlannedRewriteStep &&src) {
	for (auto &file : src.files) {
		if (!RungHasFileId(dst, file.data_file_id)) {
			dst.files.push_back(std::move(file));
		}
	}
	RefreshRewriteStepTotals(dst);
}

// Build one CALL from a cut threshold and the files newly covered by it.
// byte_budget may raise the threshold (worst-first trim) so the native CALL
// matches the planned slice. Threshold is never <= 0.
inline bool FinishRewriteRung(const std::string &band, std::vector<FileStat> files, uint64_t byte_budget,
                              double cut_threshold, double floor_threshold, double min_delete_ratio,
                              PlannedRewriteStep &out) {
	if (files.empty()) {
		return false;
	}
	std::sort(files.begin(), files.end(), SortDirtyWorstFirst);
	files = TakeWithinBudget(std::move(files), byte_budget);
	std::vector<FileStat> positive;
	positive.reserve(files.size());
	for (auto &file : files) {
		if (QualifiesForRewrite(file, min_delete_ratio)) {
			positive.push_back(std::move(file));
		}
	}
	files = std::move(positive);
	if (files.empty()) {
		return false;
	}
	double threshold = cut_threshold;
	if (!PositiveRewriteThreshold(threshold) || threshold < min_delete_ratio) {
		threshold = floor_threshold;
	}
	double min_kept = 1.0;
	for (const auto &file : files) {
		min_kept = std::min(min_kept, DeleteRatio(file));
	}
	if (min_kept > threshold) {
		threshold = min_kept;
	}
	if (threshold < floor_threshold) {
		threshold = floor_threshold;
	}
	if (threshold < min_delete_ratio) {
		threshold = min_delete_ratio;
	}
	if (!PositiveRewriteThreshold(threshold)) {
		return false;
	}
	out.band = band;
	out.files = std::move(files);
	out.delete_threshold = threshold;
	RefreshRewriteStepTotals(out);
	return true;
}

inline std::vector<double> ByteWeightedCuts(const std::vector<FileStat> &dirty, uint64_t total_bytes,
                                            uint64_t ladder_size, double floor_threshold) {
	std::vector<double> cuts;
	if (dirty.empty() || !PositiveRewriteThreshold(floor_threshold) || ladder_size == 0) {
		return cuts;
	}
	if (total_bytes == 0) {
		cuts.push_back(floor_threshold);
		return cuts;
	}
	uint64_t cum = 0;
	uint64_t next_i = 1;
	for (const auto &file : dirty) {
		cum = SaturatingAdd(cum, file.file_size_bytes);
		while (next_i <= ladder_size && CumulativeCrossesCut(cum, total_bytes, next_i, ladder_size)) {
			double cut = DeleteRatio(file);
			if (!PositiveRewriteThreshold(cut) || cut < floor_threshold) {
				cut = floor_threshold;
			}
			if (cuts.empty() || !SameThreshold(cuts.back(), cut)) {
				cuts.push_back(cut);
			}
			next_i++;
		}
	}
	if (cuts.empty()) {
		cuts.push_back(floor_threshold);
	}
	if (!PositiveRewriteThreshold(cuts.back()) || cuts.back() < floor_threshold) {
		cuts.back() = floor_threshold;
	} else if (!SameThreshold(cuts.back(), floor_threshold)) {
		if (cuts.size() < ladder_size) {
			cuts.push_back(floor_threshold);
		} else {
			cuts.back() = floor_threshold;
		}
	}
	return cuts;
}

// Files that meet min_delete_ratio, sorted worst-first (ratio DESC).
// Walk cumulative file_size_bytes; emit a threshold when the running total
// crosses total_bytes * i/N. Last cut is the smallest observed fraction in
// that pool (never 0.0, never below min_delete_ratio). Invalid ladders fail
// closed (no rungs). Duplicate thresholds merge slices instead of dropping.
inline std::vector<PlannedRewriteStep> PlanRewriteRungs(const std::vector<FileStat> &files, const RewriteLadder &ladder,
                                                        std::string *error_out = nullptr) {
	std::string error;
	if (!ValidateRewriteLadder(ladder, error)) {
		if (error_out) {
			*error_out = error;
		}
		return {};
	}
	std::vector<FileStat> dirty;
	double floor_threshold = 1.0;
	uint64_t total_bytes = 0;
	for (const auto &file : files) {
		if (!QualifiesForRewrite(file, ladder.min_delete_ratio)) {
			continue;
		}
		const double ratio = DeleteRatio(file);
		dirty.push_back(file);
		floor_threshold = std::min(floor_threshold, ratio);
		total_bytes = SaturatingAdd(total_bytes, file.file_size_bytes);
	}
	if (dirty.empty() || !PositiveRewriteThreshold(floor_threshold)) {
		return {};
	}
	if (floor_threshold < ladder.min_delete_ratio) {
		floor_threshold = ladder.min_delete_ratio;
	}
	std::sort(dirty.begin(), dirty.end(), SortDirtyWorstFirst);

	const std::vector<double> cuts = ByteWeightedCuts(dirty, total_bytes, ladder.max_rewrite_steps, floor_threshold);
	std::vector<PlannedRewriteStep> rungs;
	std::size_t file_idx = 0;
	for (std::size_t i = 0; i < cuts.size(); i++) {
		std::vector<FileStat> slice;
		const double prev = i == 0 ? 2.0 : cuts[i - 1];
		const double cut = cuts[i];
		while (file_idx < dirty.size()) {
			const double ratio = DeleteRatio(dirty[file_idx]);
			if (ratio >= cut && ratio < prev) {
				slice.push_back(dirty[file_idx]);
				file_idx++;
				continue;
			}
			if (ratio >= prev) {
				file_idx++;
				continue;
			}
			break;
		}
		// Last rung also takes any leftover dirty files so the floor CALL
		// can reach every file that met min_delete_ratio.
		if (i + 1 == cuts.size()) {
			while (file_idx < dirty.size()) {
				slice.push_back(dirty[file_idx]);
				file_idx++;
			}
		}
		PlannedRewriteStep step;
		if (FinishRewriteRung(RewriteRungName(rungs.size() + 1), std::move(slice), ladder.byte_budget, cut,
		                      floor_threshold, ladder.min_delete_ratio, step)) {
			if (!rungs.empty() && SameThreshold(rungs.back().delete_threshold, step.delete_threshold)) {
				MergeRewriteRung(rungs.back(), std::move(step));
			} else {
				rungs.push_back(std::move(step));
			}
		}
	}
	return rungs;
}

inline std::vector<PlannedRewriteStep> PlanRewriteRungs(const std::vector<FileStat> &files) {
	return PlanRewriteRungs(files, DefaultRewriteLadder());
}

inline std::string FormatRewriteStepDetails(const PlannedRewriteStep &step) {
	std::ostringstream out;
	out << "band=" << step.band << " delete_threshold=" << step.delete_threshold
	    << " planned_files=" << step.files.size() << " planned_bytes=" << step.planned_bytes
	    << " planned_deletes=" << step.planned_deletes;
	return out.str();
}

inline std::string FormatRewritePlan(const std::vector<PlannedRewriteStep> &steps) {
	if (steps.empty()) {
		return "none";
	}
	std::ostringstream out;
	for (std::vector<PlannedRewriteStep>::size_type i = 0; i < steps.size(); i++) {
		if (i > 0) {
			out << "; ";
		}
		out << FormatRewriteStepDetails(steps[i]);
	}
	return out.str();
}

// One maintain result row per planned rewrite CALL. Tests use this so dry_run
// / execute rows stay self-explanatory without a live DuckLake session.
struct RewriteRowPreview {
	std::string action;
	int64_t files_processed = 0;
	std::string details;
};

inline std::vector<RewriteRowPreview> PreviewRewriteRows(const std::vector<PlannedRewriteStep> &steps) {
	std::vector<RewriteRowPreview> rows;
	for (const auto &step : steps) {
		RewriteRowPreview row;
		row.action = step.band;
		row.files_processed = static_cast<int64_t>(step.files.size());
		row.details = FormatRewriteStepDetails(step);
		rows.push_back(std::move(row));
	}
	return rows;
}

inline std::string FormatRewriteLadderNotes(const RewriteLadder &ladder) {
	std::ostringstream out;
	out << "max_rewrite_steps=" << ladder.max_rewrite_steps << "; byte_budget=";
	if (ladder.byte_budget == 0) {
		out << "none";
	} else {
		out << ladder.byte_budget;
	}
	out << "; min_delete_ratio=" << ladder.min_delete_ratio;
	return out.str();
}

inline const MergeTier *ClassifyMerge(const FileStat &file, const std::vector<MergeTier> &tiers) {
	for (const auto &tier : tiers) {
		if (file.file_size_bytes >= tier.min_file_size && file.file_size_bytes < tier.max_file_size) {
			return &tier;
		}
	}
	return nullptr;
}

inline const MergeTier *ClassifyMerge(const FileStat &file) {
	return ClassifyMerge(file, DefaultMergeTiers());
}

inline bool FileQualifiesForMergeTier(const FileStat &file, const MergeTier &tier) {
	return file.file_size_bytes >= tier.min_file_size && file.file_size_bytes < tier.max_file_size;
}

struct TableHint {
	std::string schema_name;
	std::string table_name;
	uint64_t file_count = 0;
	uint64_t file_size_bytes = 0;
	uint64_t delete_file_count = 0;
	uint64_t delete_file_size_bytes = 0;
	uint64_t delete_count = 0;
	uint64_t deleted_bytes_weighted = 0;
	double delete_ratio = 0.0;
	std::vector<PlannedRewriteStep> rewrite_steps;
	std::string rewrite_plan = "none";
	double rewrite_threshold = 0.0;
	std::string merge_tier_hint = "none";
	std::string target_file_size;
	bool auto_compact = true;
	// Distinct inlined-delete file_ids that still exist in ducklake_data_file.
	// Ended files (end_snapshot IS NOT NULL) still get a delete file on flush.
	uint64_t inlined_delete_rows = 0;
	uint64_t inlined_delete_files_active = 0;
	uint64_t inlined_delete_files_ended = 0;
	// Non-empty when the inlined-delete table exists but could not be read
	// (permission, timeout, …). Missing table is not an error (counts stay 0).
	std::string inlined_delete_error;
};

inline uint64_t PlannedFlushDeleteFiles(const TableHint &hint) {
	return SaturatingAdd(hint.inlined_delete_files_active, hint.inlined_delete_files_ended);
}

inline std::string FormatFlushInlinedDetails(const TableHint &hint) {
	std::ostringstream out;
	out << "planned_delete_files=" << PlannedFlushDeleteFiles(hint)
	    << " inlined_delete_rows=" << hint.inlined_delete_rows
	    << " inlined_delete_files_active=" << hint.inlined_delete_files_active
	    << " inlined_delete_files_ended=" << hint.inlined_delete_files_ended;
	return out.str();
}

inline std::string FormatFlushInlinedSettingNotes() {
	return "0 = no limit; skip flush_inlined when planned delete files exceed this; negatives rejected";
}

inline std::string FormatFlushInlinedMeasureError(const TableHint &hint) {
	if (hint.inlined_delete_error.empty()) {
		return std::string();
	}
	return FormatFlushInlinedDetails(hint) + " | unread inlined-delete metadata: " + hint.inlined_delete_error;
}

inline void RefreshRewritePlan(TableHint &hint) {
	hint.rewrite_plan = FormatRewritePlan(hint.rewrite_steps);
	hint.rewrite_threshold = hint.rewrite_steps.empty() ? 0.0 : hint.rewrite_steps.front().delete_threshold;
}

// Overlay native DuckLake options (table → schema → global) on a table hint.
// rewrite_delete_threshold, when set in (0, 1], is a FULL override: one
// band=catalog CALL with that threshold. Files whose DeleteRatio is below the
// catalog value are dropped from the collapsed step. If none remain, rewrite
// is skipped. A catalog value <= 0 skips rewrite (lakemon never emits
// delete_threshold 0.0). max_rewrite_steps and per-rung byte_budget do not
// apply after collapse (they already shaped the planned rungs). Files below
// min_delete_ratio were never planned and stay out. auto_compact is honored
// by maintain.
inline void ApplyNativeOptions(TableHint &hint, const std::vector<OptionBinding> &options) {
	const ResolvedOption rewrite =
	    ResolveOption(options, "rewrite_delete_threshold", hint.schema_name, hint.table_name);
	double catalog_value = 0;
	bool skip_non_positive = false;
	bool skip_catalog_empty = false;
	if (rewrite.found && TryParseDouble(rewrite.value, catalog_value)) {
		if (catalog_value <= 0.0) {
			hint.rewrite_steps.clear();
			skip_non_positive = true;
		} else if (PositiveRewriteThreshold(catalog_value) && !hint.rewrite_steps.empty()) {
			PlannedRewriteStep step;
			step.band = "catalog";
			step.delete_threshold = catalog_value;
			for (const auto &existing : hint.rewrite_steps) {
				for (const auto &file : existing.files) {
					if (DeleteRatio(file) >= catalog_value) {
						step.files.push_back(file);
					}
				}
			}
			hint.rewrite_steps.clear();
			if (step.files.empty()) {
				skip_catalog_empty = true;
			} else {
				RefreshRewriteStepTotals(step);
				hint.rewrite_steps.push_back(std::move(step));
			}
		}
	}
	if (skip_non_positive) {
		hint.rewrite_plan = kSkipNonPositiveRewriteThreshold;
		hint.rewrite_threshold = 0.0;
	} else if (skip_catalog_empty) {
		hint.rewrite_plan = kSkipCatalogThresholdMiss;
		hint.rewrite_threshold = 0.0;
	} else {
		RefreshRewritePlan(hint);
	}
	const ResolvedOption compact = ResolveOption(options, "auto_compact", hint.schema_name, hint.table_name);
	hint.auto_compact = EffectiveAutoCompact(compact);
	const ResolvedOption target = ResolveOption(options, "target_file_size", hint.schema_name, hint.table_name);
	if (target.found) {
		hint.target_file_size = target.value;
	}
}

// File inventory only sees parquet. Live tables that still have only inlined
// inserts must still get a hint so flush_inlined is planned.
inline bool SameTableHint(const TableHint &left, const TableHint &right) {
	return left.schema_name == right.schema_name && left.table_name == right.table_name;
}

inline void AddMissingLiveTableHints(std::vector<TableHint> &hints, const std::vector<TableHint> &live_tables,
                                     const std::vector<OptionBinding> &options) {
	for (const auto &live : live_tables) {
		if (live.schema_name.empty() && live.table_name.empty()) {
			continue;
		}
		bool found = false;
		for (const auto &hint : hints) {
			if (SameTableHint(hint, live)) {
				found = true;
				break;
			}
		}
		if (found) {
			continue;
		}
		TableHint hint;
		hint.schema_name = live.schema_name;
		hint.table_name = live.table_name;
		ApplyNativeOptions(hint, options);
		hints.push_back(std::move(hint));
	}
}

inline TableHint SummarizeTable(const std::vector<FileStat> &files, const ActivePolicy &policy) {
	TableHint hint;
	if (files.empty()) {
		return hint;
	}
	hint.schema_name = files.front().schema_name;
	hint.table_name = files.front().table_name;
	uint64_t records = 0;
	std::vector<uint64_t> merge_counts(policy.tiers.size(), 0);
	for (const auto &file : files) {
		hint.file_count++;
		hint.file_size_bytes += file.file_size_bytes;
		hint.delete_count += file.delete_count;
		hint.delete_file_size_bytes += file.delete_file_size_bytes;
		if (file.delete_count > 0 || file.delete_file_size_bytes > 0) {
			hint.delete_file_count++;
		}
		hint.deleted_bytes_weighted += DeletedBytes(file);
		records += file.record_count;
		for (std::vector<MergeTier>::size_type i = 0; i < policy.tiers.size(); i++) {
			if (FileQualifiesForMergeTier(file, policy.tiers[i])) {
				merge_counts[i]++;
				break;
			}
		}
	}
	hint.delete_ratio =
	    records == 0 ? 0.0 : std::min(1.0, static_cast<double>(hint.delete_count) / static_cast<double>(records));
	hint.rewrite_steps = PlanRewriteRungs(files, policy.rewrite);
	RefreshRewritePlan(hint);
	for (std::vector<MergeTier>::size_type i = 0; i < policy.tiers.size(); i++) {
		if (merge_counts[i] >= 2ULL) {
			hint.merge_tier_hint = policy.tiers[i].name;
			break;
		}
	}
	return hint;
}

inline TableHint SummarizeTable(const std::vector<FileStat> &files) {
	return SummarizeTable(files, DefaultPolicy());
}

} // namespace policy
} // namespace lakemon
