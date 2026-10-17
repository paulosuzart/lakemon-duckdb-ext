#include "expect.hpp"
#include "lakemon_diagnostics.hpp"
#include "lakemon_pipeline.hpp"
#include "lakemon_policy.hpp"
#include "lakemon_sql.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

using namespace lakemon::policy;

static FileStat MakeFile(uint64_t id, uint64_t deletes, uint64_t records, uint64_t bytes) {
	FileStat file;
	file.data_file_id = id;
	file.delete_count = deletes;
	file.record_count = records;
	file.file_size_bytes = bytes;
	return file;
}

static FileStat MakeFile(const std::string &schema, const std::string &table, uint64_t id, uint64_t deletes,
                         uint64_t records, uint64_t bytes) {
	FileStat file = MakeFile(id, deletes, records, bytes);
	file.schema_name = schema;
	file.table_name = table;
	return file;
}

// Rejected: equal-width delete_threshold steps (0.1, 0.2, …).
static uint32_t EqualWidthRatioIndex(const FileStat &file, double width) {
	return static_cast<uint32_t>((DeleteRatio(file) / width) + 1e-9);
}

// Rejected: equal-count splits that ignore deletes (here: by size).
static std::vector<std::vector<FileStat>> EqualCountChunks(std::vector<FileStat> files, std::size_t chunk) {
	std::sort(files.begin(), files.end(),
	          [](const FileStat &a, const FileStat &b) { return a.file_size_bytes < b.file_size_bytes; });
	std::vector<std::vector<FileStat>> out;
	for (std::size_t i = 0; i < files.size(); i += chunk) {
		std::vector<FileStat> part;
		for (std::size_t j = i; j < files.size() && j < i + chunk; j++) {
			part.push_back(files[j]);
		}
		out.push_back(std::move(part));
	}
	return out;
}

static void TestDefaultLadderIsByteWeighted() {
	const auto ladder = DefaultRewriteLadder();
	std::string error;
	Expect(ValidateRewriteLadder(ladder, error), "default ladder validates");
	Expect(ladder.max_rewrite_steps == 3, "default max_rewrite_steps is 3");
	Expect(ladder.max_rewrite_steps <= kMaxRewriteStepsLimit, "default is within 1–16");
	Expect(ladder.byte_budget == 0, "byte_budget unset");
	Expect(std::abs(ladder.min_delete_ratio - 0.01) < 1e-15, "default min_delete_ratio is 0.01");
}

static void TestValidateRejectsZeroAndOversizeSteps() {
	std::string error;
	RewriteLadder zero_steps = DefaultRewriteLadder();
	zero_steps.max_rewrite_steps = 0;
	Expect(!ValidateRewriteLadder(zero_steps, error), "max_rewrite_steps 0 rejected");
	Expect(error.find("max_rewrite_steps") != std::string::npos, "max_rewrite_steps message");

	error.clear();
	RewriteLadder too_many = DefaultRewriteLadder();
	too_many.max_rewrite_steps = 17;
	Expect(!ValidateRewriteLadder(too_many, error), "max_rewrite_steps 17 rejected");
	Expect(error.find("max_rewrite_steps") != std::string::npos, "upper bound message");

	error.clear();
	RewriteLadder zero_ratio = DefaultRewriteLadder();
	zero_ratio.min_delete_ratio = 0.0;
	Expect(!ValidateRewriteLadder(zero_ratio, error), "min_delete_ratio 0 rejected");
	Expect(error.find("min_delete_ratio") != std::string::npos, "min_delete_ratio message");

	error.clear();
	RewriteLadder over_ratio = DefaultRewriteLadder();
	over_ratio.min_delete_ratio = 1.5;
	Expect(!ValidateRewriteLadder(over_ratio, error), "min_delete_ratio > 1 rejected");
}

static void TestEqualSizeFilesCutThreeThresholds() {
	const auto files = std::vector<FileStat>{
	    MakeFile(1, 90, 100, 100), // 0.90
	    MakeFile(2, 50, 100, 100), // 0.50
	    MakeFile(3, 10, 100, 100), // 0.10
	};
	const auto rungs = PlanRewriteRungs(files);
	Expect(rungs.size() == 3, "equal-byte files yield three rungs");
	Expect(rungs[0].band == "rung_1" && rungs[1].band == "rung_2" && rungs[2].band == "rung_3", "rung names");
	Expect(std::abs(rungs[0].delete_threshold - 0.90) < 1e-12, "first cut is worst fraction");
	Expect(std::abs(rungs[1].delete_threshold - 0.50) < 1e-12, "second cut is mid fraction");
	Expect(std::abs(rungs[2].delete_threshold - 0.10) < 1e-12, "last cut is the positive floor");
	Expect(rungs[0].files[0].data_file_id == 1, "worst file on first rung");
	Expect(rungs[1].files[0].data_file_id == 2, "mid file on second rung");
	Expect(rungs[2].files[0].data_file_id == 3, "lowest positive fraction on last rung");
	for (const auto &step : rungs) {
		Expect(step.delete_threshold > 0.0, "no planned rung uses delete_threshold 0");
		Expect(lakemon::ShouldEmitRewriteCall(step.delete_threshold), "maintain would emit this CALL");
	}
}

static void TestLadderNeverEndsAtZero() {
	const auto files = std::vector<FileStat>{
	    MakeFile(1, 90, 100, 100),
	    MakeFile(2, 50, 100, 100),
	    MakeFile(3, 10, 100, 100),
	    MakeFile(4, 0, 100, 1000), // clean — ignored
	};
	const auto rungs = PlanRewriteRungs(files);
	Expect(!rungs.empty(), "dirty files still plan");
	Expect(std::abs(rungs.back().delete_threshold - 0.10) < 1e-12, "floor is the smallest positive fraction");
	for (const auto &step : rungs) {
		Expect(step.delete_threshold > 0.0, "ladder never emits 0.0");
	}
}

static void TestNoDeleteDataSkipsRewrite() {
	Expect(PlanRewriteRungs({}).empty(), "empty input");
	const auto clean = std::vector<FileStat>{MakeFile(1, 0, 1000, 10), MakeFile(2, 0, 1000, 10)};
	Expect(PlanRewriteRungs(clean).empty(), "no delete data yields no rungs");
}

static void TestHugeLowRatioFileDoesNotHideSmallHotFile() {
	// Bytes are dominated by a low-ratio file; early cuts still fire on the
	// worst files first, and the last cut is the observed floor (0.05).
	const auto files = std::vector<FileStat>{
	    MakeFile(1, 50000, 1000000, 800000000), // 0.05 — most bytes
	    MakeFile(2, 20, 100, 8000),             // 0.20
	    MakeFile(3, 8000, 10000, 4000000),      // 0.80
	};
	const auto rungs = PlanRewriteRungs(files);
	Expect(!rungs.empty(), "dirty files plan");
	Expect(rungs[0].files[0].data_file_id == 3, "worst fraction runs first");
	Expect(std::abs(rungs.back().delete_threshold - 0.05) < 1e-12, "last cut is the 0.05 floor");
	Expect(rungs.back().delete_threshold > 0.0, "floor is not 0.0");
}

static void TestEqualWidthRatioStepsMisgroupTheSameFiles() {
	const FileStat big = MakeFile(1, 50000, 1000000, 800000000);
	const FileStat tiny = MakeFile(2, 20, 100, 8000);
	const FileStat hot = MakeFile(3, 8000, 10000, 4000000);

	const uint32_t tiny_idx = EqualWidthRatioIndex(tiny, 0.1);
	const uint32_t big_idx = EqualWidthRatioIndex(big, 0.1);
	const uint32_t hot_idx = EqualWidthRatioIndex(hot, 0.1);
	Expect(tiny_idx == 2, "tiny 0.20 lands in width bucket 2");
	Expect(big_idx == 0, "big 0.05 lands in width bucket 0");
	Expect(hot_idx == 8, "hot 0.80 lands in width bucket 8");
	Expect(tiny_idx > big_idx, "equal-width ratio buckets rank the 20-delete file above the 50k-delete file");

	const auto rungs = PlanRewriteRungs({big, tiny, hot});
	Expect(!rungs.empty(), "byte-weighted plan is non-empty");
	Expect(rungs[0].files[0].data_file_id == 3, "worst fraction is first, not the equal-width bucket");
	Expect(std::abs(rungs.back().delete_threshold - 0.05) < 1e-12, "floor is min positive fraction");
}

static void TestEqualCountSizeSplitsMixCleanAndDirtyFiles() {
	const auto files = std::vector<FileStat>{
	    MakeFile(1, 0, 10000, 100),
	    MakeFile(2, 12000, 20000, 200),
	    MakeFile(3, 0, 10000, 300),
	    MakeFile(4, 15000, 20000, 400),
	};
	const auto chunks = EqualCountChunks(files, 2);
	Expect(chunks.size() == 2, "two size chunks");
	Expect(chunks[0][0].data_file_id == 1 && chunks[0][1].data_file_id == 2, "size order: clean-small + dirty-a");
	Expect(chunks[1][0].data_file_id == 3 && chunks[1][1].data_file_id == 4, "size order: clean-mid + dirty-b");
	bool mixed = true;
	for (const auto &chunk : chunks) {
		bool has_clean = false;
		bool has_dirty = false;
		for (const auto &file : chunk) {
			if (file.delete_count == 0) {
				has_clean = true;
			}
			if (file.delete_count >= 10000) {
				has_dirty = true;
			}
		}
		if (!has_clean || !has_dirty) {
			mixed = false;
		}
	}
	Expect(mixed, "equal-count size splits mix 0-delete files with high-delete files");

	const auto rungs = PlanRewriteRungs(files);
	Expect(!rungs.empty(), "dirty files plan");
	uint64_t planned = 0;
	for (const auto &step : rungs) {
		planned += step.files.size();
		for (const auto &file : step.files) {
			Expect(file.delete_count > 0, "clean files are never planned");
		}
	}
	Expect(planned == 2, "only the two dirty files are planned");
}

static void TestByteBudgetCapsARung() {
	RewriteLadder ladder = DefaultRewriteLadder();
	ladder.byte_budget = 160;
	ladder.max_rewrite_steps = 1;
	const auto files = std::vector<FileStat>{
	    MakeFile(1, 90, 100, 80),
	    MakeFile(2, 80, 100, 80),
	    MakeFile(3, 70, 100, 80),
	};
	const auto rungs = PlanRewriteRungs(files, ladder);
	Expect(rungs.size() == 1, "N=1 is one rung");
	Expect(rungs[0].files.size() == 2, "budget keeps two files (80+80=160; third would be 240)");
	Expect(rungs[0].files[0].data_file_id == 1 && rungs[0].files[1].data_file_id == 2, "worst-first under budget");
	Expect(rungs[0].planned_bytes == 160, "planned bytes after budget");
	Expect(rungs[0].delete_threshold > 0.0, "budgeted rung stays positive");
}

static void TestFirstFileMayExceedBudgetToMakeProgress() {
	RewriteLadder ladder = DefaultRewriteLadder();
	ladder.byte_budget = 50;
	ladder.max_rewrite_steps = 1;
	const auto files = std::vector<FileStat>{
	    MakeFile(1, 90, 100, 500),
	    MakeFile(2, 80, 100, 10),
	};
	const auto rungs = PlanRewriteRungs(files, ladder);
	Expect(rungs[0].files.size() == 1, "only the first file");
	Expect(rungs[0].files[0].data_file_id == 1, "huge file still planned");
}

static void TestZeroRecordFileWithDeletesHasRatioOne() {
	Expect(DeleteRatio(MakeFile(1, 5, 0, 1)) == 1.0, "ghost file ratio 1");
	Expect(DeleteRatio(MakeFile(2, 0, 0, 1)) == 0.0, "empty file ratio 0");
	FileStat listed;
	listed.delete_file_size_bytes = 16;
	listed.record_count = 0;
	listed.delete_count = 0;
	listed.file_size_bytes = 80;
	Expect(std::abs(DeleteRatio(listed) - 0.2) < 1e-12, "list_files uses size ratio, not 1.0");
	Expect(HasPositiveDeleteFraction(listed), "list_files delete file is dirty");
}

static void TestMinDeleteRatioExcludesGiantNearClean() {
	const auto files = std::vector<FileStat>{
	    MakeFile(1, 1, 1000000, 800000000), // ~1e-6 — huge near-clean
	    MakeFile(2, 90, 100, 100),
	    MakeFile(3, 50, 100, 100),
	    MakeFile(4, 10, 100, 100),
	};
	const auto rungs = PlanRewriteRungs(files);
	Expect(rungs.size() == 3, "giant below min_delete_ratio does not collapse the ladder");
	Expect(std::abs(rungs[0].delete_threshold - 0.90) < 1e-12, "first cut stays 0.90");
	Expect(std::abs(rungs.back().delete_threshold - 0.10) < 1e-12, "floor is 0.10 not 1e-6");
	for (const auto &step : rungs) {
		for (const auto &file : step.files) {
			Expect(file.data_file_id != 1, "giant near-clean file is excluded");
			Expect(DeleteRatio(file) >= kDefaultMinDeleteRatio, "rung files meet min_delete_ratio");
		}
		Expect(step.delete_threshold > 0.0, "planned threshold stays positive");
	}

	RewriteLadder open = DefaultRewriteLadder();
	open.min_delete_ratio = 1e-9;
	const auto unfiltered = PlanRewriteRungs(files, open);
	Expect(!unfiltered.empty(), "tiny floor still plans");
	Expect(std::abs(unfiltered.back().delete_threshold - 1e-6) < 1e-12,
	       "without a 0.01 floor the giant would set the last cut");
}

static void TestInvalidLadderFailsClosed() {
	RewriteLadder bad;
	bad.max_rewrite_steps = 0;
	std::string error;
	Expect(PlanRewriteRungs({}, bad, &error).empty(), "invalid ladder returns no rungs");
	Expect(!error.empty(), "invalid ladder records an error");
}

static void TestMaxRewriteStepsIsLadderSize() {
	const auto files = std::vector<FileStat>{
	    MakeFile(1, 90, 100, 100),
	    MakeFile(2, 50, 100, 100),
	    MakeFile(3, 10, 100, 100),
	};
	RewriteLadder one = DefaultRewriteLadder();
	one.max_rewrite_steps = 1;
	const auto single = PlanRewriteRungs(files, one);
	Expect(single.size() == 1, "N=1 emits one CALL");
	Expect(std::abs(single[0].delete_threshold - 0.10) < 1e-12, "single cut is the floor (all bytes)");

	RewriteLadder two = DefaultRewriteLadder();
	two.max_rewrite_steps = 2;
	const auto pair = PlanRewriteRungs(files, two);
	Expect(pair.size() == 2, "N=2 emits two cuts");
	Expect(pair[0].delete_threshold > pair[1].delete_threshold, "worst-first then floor");
	Expect(std::abs(pair[1].delete_threshold - 0.10) < 1e-12, "last cut is the positive floor");
}

static void TestZeroRatioSliceIsSkipped() {
	FileStat zero = MakeFile(1, 0, 1000, 100);
	PlannedRewriteStep out;
	Expect(!FinishRewriteRung("rung_1", {zero}, 0, 0.0, 0.1, 0.01, out), "min ratio 0 skips the rung");
	Expect(!lakemon::ShouldEmitRewriteCall(0.0), "threshold 0 is never a lakemon rewrite CALL");
	Expect(!lakemon::ShouldEmitRewriteCall(-0.1), "negative threshold is never a lakemon rewrite CALL");
	Expect(lakemon::ShouldEmitRewriteCall(0.05), "positive threshold may emit");
}

static void TestZeroRatioFileDoesNotPoisonPositiveRung() {
	FileStat dirty = MakeFile(1, 20000, 40000, 100);
	FileStat zero = MakeFile(2, 0, 1000, 50);
	PlannedRewriteStep out;
	Expect(FinishRewriteRung("rung_1", {dirty, zero}, 0, 0.5, 0.5, 0.01, out), "positive files remain");
	Expect(out.files.size() == 1 && out.files[0].data_file_id == 1, "zero-ratio file is dropped");
	Expect(out.delete_threshold > 0.0, "threshold stays positive");
	Expect(lakemon::ShouldEmitRewriteCall(out.delete_threshold), "remaining rung is emittable");
}

static void TestDuplicateThresholdMergesFiles() {
	PlannedRewriteStep first;
	Expect(FinishRewriteRung("rung_1", {MakeFile(1, 50, 100, 40)}, 0, 0.5, 0.5, 0.01, first), "first slice");
	PlannedRewriteStep second;
	Expect(FinishRewriteRung("rung_1", {MakeFile(2, 50, 100, 60), MakeFile(1, 50, 100, 40)}, 0, 0.5, 0.5, 0.01, second),
	       "second slice shares a file id");
	Expect(SameThreshold(first.delete_threshold, second.delete_threshold), "same data-driven threshold");
	MergeRewriteRung(first, std::move(second));
	Expect(first.files.size() == 2, "duplicate threshold merges slices instead of dropping");
	Expect(first.planned_bytes == 100, "planned bytes refreshed");
	Expect(first.planned_deletes == 100, "planned deletes refreshed");
	bool saw1 = false;
	bool saw2 = false;
	for (const auto &file : first.files) {
		saw1 = saw1 || file.data_file_id == 1;
		saw2 = saw2 || file.data_file_id == 2;
	}
	Expect(saw1 && saw2, "dedupe keeps both file ids");
}

static void TestListFilesSizeRatioCanBeExcluded() {
	FileStat listed;
	listed.data_file_id = 9;
	listed.file_size_bytes = 1000000;
	listed.delete_file_size_bytes = 100; // 1e-4
	listed.record_count = 0;
	listed.delete_count = 0;
	Expect(std::abs(DeleteRatio(listed) - 1e-4) < 1e-12, "conservative size ratio");
	Expect(!QualifiesForRewrite(listed, kDefaultMinDeleteRatio), "below default min_delete_ratio");
	const auto rungs = PlanRewriteRungs({listed, MakeFile(1, 50, 100, 100)});
	Expect(rungs.size() == 1, "real dirty file still plans");
	Expect(rungs[0].files.size() == 1 && rungs[0].files[0].data_file_id == 1, "list_files near-clean is excluded");
}

static void TestDeletedBytesUsesCount() {
	FileStat heavy;
	heavy.file_size_bytes = 20 * kMiB;
	heavy.record_count = 20000;
	heavy.delete_count = 4000;
	Expect(DeletedBytes(heavy) == 4 * kMiB, "weighted bytes = size * delete_count/records");
	Expect(std::abs(DeleteRatio(heavy) - 0.20) < 1e-9, "ratio 0.20");
}

static void TestMultiRungPlanEmitsDistinctCalls() {
	std::vector<FileStat> files;
	files.push_back(MakeFile("main", "events", 1, 90, 100, 100));
	files.push_back(MakeFile("main", "events", 2, 50, 100, 100));
	files.push_back(MakeFile("main", "events", 3, 10, 100, 100));
	const auto hint = SummarizeTable(files);
	Expect(hint.rewrite_steps.size() == 3, "dry-run plan has three rewrite steps");
	Expect(hint.rewrite_steps[0].band == "rung_1" && hint.rewrite_steps[2].band == "rung_3", "rung_1 then later rungs");
	Expect(hint.rewrite_steps[0].delete_threshold != hint.rewrite_steps[1].delete_threshold,
	       "thresholds are data-driven and distinct");
	Expect(hint.rewrite_plan.find("band=rung_1") != std::string::npos, "plan surfaces first rung");
	Expect(hint.rewrite_plan.find("band=rung_2") != std::string::npos, "plan surfaces second rung");
	Expect(hint.rewrite_plan.find("planned_files=") != std::string::npos, "plan surfaces file counts");
	Expect(hint.rewrite_plan.find("delete_threshold=0") == std::string::npos ||
	           hint.rewrite_plan.find("delete_threshold=0.1") != std::string::npos,
	       "plan may show 0.1 but not a lone 0.0 cut");
	for (const auto &step : hint.rewrite_steps) {
		Expect(step.delete_threshold > 0.0, "summarize never plans 0.0");
	}

	lakemon::TableRef ref;
	ref.schema = "main";
	ref.table = "events";
	const std::string first_sql = lakemon::RewriteDataFilesCall("lake", ref, hint.rewrite_steps[0].delete_threshold);
	const std::string last_sql = lakemon::RewriteDataFilesCall("lake", ref, hint.rewrite_steps[2].delete_threshold);
	Expect(first_sql != last_sql, "each rung is its own CALL");
	Expect(first_sql.find("delete_threshold => 0.9") != std::string::npos, "first CALL uses derived 0.9");
	Expect(last_sql.find("delete_threshold => 0.1") != std::string::npos, "last CALL uses the positive floor");

	const auto preview = PreviewRewriteRows(hint.rewrite_steps);
	Expect(preview.size() == 3, "maintain emits one result row per rung, not one opaque rewrite");
	Expect(preview[0].action == "rung_1" && preview[2].action == "rung_3", "action is the computed rung");
	Expect(preview[0].files_processed == 1 && preview[2].files_processed == 1, "files_processed is planned files");
	Expect(preview[0].details.find("delete_threshold=0.9") != std::string::npos, "first row shows data-driven threshold");
	Expect(preview[0].details.find("planned_files=1") != std::string::npos, "first row shows planned_files");
	Expect(preview[2].details.find("delete_threshold=0.1") != std::string::npos, "last row shows the floor");
	Expect(preview[0].details.find("band=rung_1") != std::string::npos, "each row names its rung");

	const std::string skip_first = preview[0].details + " | auto_compact=false";
	Expect(skip_first.find("band=rung_1") != std::string::npos, "skip/error rows keep the planned rung");
	const std::string ok_first = preview[0].details + " | created=1";
	Expect(ok_first.find("delete_threshold=0.9") != std::string::npos && ok_first.find("created=1") != std::string::npos,
	       "execute ok keeps the plan and records how the CALL affected the table");
}

static void TestCutsUseBytesNotDeleteCount() {
	// Same delete counts, very different sizes: cuts follow bytes.
	const auto files = std::vector<FileStat>{
	    MakeFile(1, 100, 200, 10),   // 0.50, tiny
	    MakeFile(2, 100, 1000, 100), // 0.10, most bytes
	    MakeFile(3, 100, 125, 90),   // 0.80, mid bytes
	};
	RewriteLadder ladder = DefaultRewriteLadder();
	ladder.max_rewrite_steps = 2;
	const auto rungs = PlanRewriteRungs(files, ladder);
	Expect(rungs.size() == 2, "two byte-weighted cuts");
	Expect(rungs[0].files[0].data_file_id == 3, "worst fraction first regardless of delete_count");
	Expect(std::abs(rungs.back().delete_threshold - 0.10) < 1e-12, "floor is 0.10, not a delete_count band");
}

static void TestMergeTiers() {
	FileStat micro;
	micro.file_size_bytes = 200 * 1024;
	Expect(ClassifyMerge(micro) && std::string(ClassifyMerge(micro)->name) == "micro", "micro tier");
	FileStat small;
	small.file_size_bytes = 3 * kMiB;
	Expect(ClassifyMerge(small) && std::string(ClassifyMerge(small)->name) == "small", "small tier");
	FileStat medium;
	medium.file_size_bytes = 20 * kMiB;
	Expect(ClassifyMerge(medium) && std::string(ClassifyMerge(medium)->name) == "medium", "medium tier");
	FileStat large;
	large.file_size_bytes = 80 * kMiB;
	Expect(ClassifyMerge(large) == nullptr, "already-large files skip merge tiers");
}

static void TestTableHintMerge() {
	std::vector<FileStat> files;
	for (int i = 0; i < 5; i++) {
		FileStat f;
		f.schema_name = "s";
		f.table_name = "t";
		f.file_size_bytes = 100 * 1024;
		f.record_count = 10;
		files.push_back(f);
	}
	const auto hint = SummarizeTable(files);
	Expect(hint.merge_tier_hint == "micro", "five micro files hint micro merge");
	Expect(hint.file_count == 5, "file_count");
	Expect(hint.rewrite_steps.empty() && hint.rewrite_plan == "none", "no deletes → no rewrite steps");
}

static void TestOneFileDoesNotHintMerge() {
	FileStat f;
	f.schema_name = "s";
	f.table_name = "t";
	f.file_size_bytes = 100 * 1024;
	const auto hint = SummarizeTable(std::vector<FileStat>(1, f));
	Expect(hint.merge_tier_hint == "none", "one file stays below the 2ULL merge hint");
}

static void TestPipelineStepsAreIndependent() {
	Expect(lakemon::InventoryFailureStopsPipeline(), "hard inventory failure stops rewrite/merge");
	Expect(!lakemon::FlushErrorSkipsStep("rewrite"), "flush error does not skip rewrite");
	Expect(!lakemon::FlushErrorSkipsStep("merge"), "flush error does not skip merge");
	Expect(!lakemon::StepErrorStopsBatch("flush_inlined"),
	       "flush_inlined execute error does not fail-fast the rest of the batch");
	Expect(lakemon::StepErrorStopsBatch("rewrite"), "rewrite execute error still fail-fasts");
	Expect(lakemon::StepErrorStopsBatch("merge"), "merge execute error still fail-fasts");
	Expect(!lakemon::TableMaintainIncludesRetention(),
	       "lakemon_maintain must not expire snapshots or clean old/orphan files");
	Expect(!lakemon::GlobalMaintainIncludesTableWork(),
	       "lakemon_maintain_global must not flush, rewrite, or merge");
	Expect(lakemon::TableFunctionRunsDuckLakeCalls(),
	       "CALL / SELECT bind_replace executes planned DuckLake CALLs");
	Expect(!lakemon::FlushErrorSkipsStep("expire_snapshots"), "flush error does not skip expire");
	Expect(!lakemon::FlushErrorSkipsStep("cleanup_old_files"), "flush error does not skip cleanup");
	Expect(!lakemon::FlushErrorSkipsStep("delete_orphaned_files"), "flush error does not skip orphan cleanup");
}

static void TestGlobalExpireCleanupSkipSemantics() {
	Expect(std::string(lakemon::ExpireSkipReason(true, false)) == "skip_expire", "skip_expire wins");
	Expect(std::string(lakemon::ExpireSkipReason(true, true)) == "skip_expire", "skip_expire wins even if unset");
	Expect(std::string(lakemon::ExpireSkipReason(false, true)) == "expire_older_than not set",
	       "unset interval skips expire");
	Expect(lakemon::ExpireSkipReason(false, false) == nullptr, "set interval runs expire");
	Expect(!lakemon::ShouldRunCleanup(true), "skip_cleanup skips old-file and orphan cleanup");
	Expect(lakemon::ShouldRunCleanup(false), "cleanup runs unless skipped");
}

static void TestCumulativeCrossesCutIsOverflowSafe() {
	Expect(!CumulativeCrossesCut(0, 10, 1, 3), "zero cumulative does not cross");
	Expect(!CumulativeCrossesCut(5, 0, 1, 3), "zero total does not cross");
	Expect(!CumulativeCrossesCut(5, 10, 0, 3), "zero cut index does not cross");
	Expect(!CumulativeCrossesCut(5, 10, 1, 0), "zero ladder size does not cross");
	Expect(CumulativeCrossesCut(5, 10, 1, 2), "half the bytes crosses 1/2");
	Expect(!CumulativeCrossesCut(4, 10, 1, 2), "below half does not cross 1/2");
	Expect(CumulativeCrossesCut(10, 10, 3, 3), "full bytes crosses the last cut");

	// 2^63 * 4 and 2^63 * 3 both overflow uint64. Naive wrap compares 0 >= 2^63.
	const uint64_t half = 1ULL << 63;
	Expect(CumulativeCrossesCut(half, half, 3, 4), "full bytes crosses 3/4 without __int128");
	Expect(!CumulativeCrossesCut(half / 2, half, 3, 4), "quarter of bytes does not cross 3/4");
	Expect(CumulativeCrossesCut(~0ULL, ~0ULL, 1, 1), "max uint64 full product still compares");
	Expect(!CumulativeCrossesCut(1, ~0ULL, 1, 2), "tiny cumulative vs huge total stays below 1/2");
}

static void TestFlushInlinedGuardAndDetails() {
	Expect(!lakemon::FlushInlinedExceedsLimit(0, 500), "empty backlog does not skip");
	Expect(!lakemon::FlushInlinedExceedsLimit(500, 500), "equal to the limit still flushes");
	Expect(lakemon::FlushInlinedExceedsLimit(501, 500), "one over the default limit skips");
	Expect(!lakemon::FlushInlinedExceedsLimit(10000, 0), "0 is no limit");
	Expect(!lakemon::FlushInlinedExceedsLimit(2, -1), "negative is no limit");
	Expect(lakemon::FlushInlinedExceedsLimit(2, 1), "low threshold skips");
	Expect(!lakemon::FlushInlinedExceedsLimit(1, 1), "planned == limit still flushes");

	TableHint hint;
	hint.inlined_delete_rows = 12;
	hint.inlined_delete_files_active = 3;
	hint.inlined_delete_files_ended = 9;
	Expect(PlannedFlushDeleteFiles(hint) == 12, "planned delete files are active + ended");
	const std::string details = FormatFlushInlinedDetails(hint);
	Expect(details.find("planned_delete_files=12") != std::string::npos, "details include planned count");
	Expect(details.find("inlined_delete_rows=12") != std::string::npos, "details include row count");
	Expect(details.find("inlined_delete_files_active=3") != std::string::npos, "details include active files");
	Expect(details.find("inlined_delete_files_ended=9") != std::string::npos, "details include ended files");

	const std::string skip = lakemon::FlushInlinedSkipReason(12, 5);
	Expect(skip.find("planned_delete_files=12") != std::string::npos, "skip names the planned count");
	Expect(skip.find("lakemon_flush_inlined_max_delete_files=5") != std::string::npos, "skip names the setting");
	Expect(skip.find("ducklake_expire_snapshots") != std::string::npos, "skip recommends expire");

	Expect(FormatFlushInlinedSettingNotes().find("0 = no limit") != std::string::npos, "policy notes mention 0");
	Expect(std::string(kKindSetting) == "setting", "policy kind for the setting row");
	Expect(std::string(kFlushInlinedMaxDeleteFilesName) == "flush_inlined_max_delete_files",
	       "policy name matches the setting suffix");

	std::string setting_error;
	Expect(lakemon::ValidFlushInlinedMaxDeleteFiles(0, setting_error), "0 is valid (no limit)");
	Expect(lakemon::ValidFlushInlinedMaxDeleteFiles(500, setting_error), "default is valid");
	Expect(!lakemon::ValidFlushInlinedMaxDeleteFiles(-1, setting_error), "negatives are rejected");
	Expect(setting_error.find("must be >= 0") != std::string::npos, "negative error names the bound");

	Expect(lakemon::LooksLikeMissingInlinedDeleteTable(
	           "Catalog Error: Table with name ducklake_inlined_delete_3 does not exist!",
	           "ducklake_inlined_delete_3"),
	       "missing inlined-delete table is recognized");
	Expect(lakemon::LooksLikeMissingInlinedDeleteTable("relation \"ducklake_inlined_delete_3\" does not exist",
	                                                  "ducklake_inlined_delete_3"),
	       "Postgres missing-relation wording is recognized");
	Expect(!lakemon::LooksLikeMissingInlinedDeleteTable(
	           "ERROR: permission denied for table ducklake_inlined_delete_3", "ducklake_inlined_delete_3"),
	       "permission errors are not missing-table");
	Expect(!lakemon::LooksLikeMissingInlinedDeleteTable("ERROR: lock timeout", "ducklake_inlined_delete_3"),
	       "timeout without the table name is not missing-table");
	TableHint unread;
	unread.inlined_delete_error = "permission denied";
	const std::string measure = FormatFlushInlinedMeasureError(unread);
	Expect(measure.find("unread inlined-delete metadata") != std::string::npos, "measure error is explicit");
	Expect(measure.find("permission denied") != std::string::npos, "measure error keeps the cause");
}

static void TestInlinedOnlyTablesGetHints() {
	std::vector<TableHint> hints;
	TableHint live;
	live.schema_name = "bronze_financial";
	live.table_name = "subscription";
	AddMissingLiveTableHints(hints, {live}, {});
	Expect(hints.size() == 1, "inlined-only live table is inventoried for flush");
	Expect(hints[0].schema_name == "bronze_financial", "live table schema");
	Expect(hints[0].table_name == "subscription", "live table name");
	Expect(hints[0].file_count == 0, "no parquet files yet");
	Expect(hints[0].auto_compact, "default auto_compact on an inlined-only table");
	AddMissingLiveTableHints(hints, {live}, {});
	Expect(hints.size() == 1, "already inventoried table is not duplicated");

	TableHint existing;
	existing.schema_name = "main";
	existing.table_name = "events";
	existing.file_count = 2;
	std::vector<TableHint> with_files = {existing};
	TableHint also_live = existing;
	AddMissingLiveTableHints(with_files, {also_live}, {});
	Expect(with_files.size() == 1, "file-backed table is not replaced");
	Expect(with_files[0].file_count == 2, "existing file stats are kept");
}

static void TestFormatInventoryDiagnosticsJoinsAll() {
	lakemon::InventoryDiagnostic first;
	first.schema_name = "main";
	first.table_name = "events";
	first.source = "metadata";
	first.message = "missing delete_count";
	lakemon::InventoryDiagnostic second;
	second.schema_name = "sales";
	second.table_name = "orders";
	second.source = "list_files";
	second.message = "lakemon: list_files returned no result";
	const std::string joined = lakemon::FormatInventoryDiagnostics({first, second});
	Expect(joined.find("main.events (metadata): missing delete_count") != std::string::npos,
	       "first diagnostic is kept");
	Expect(joined.find("sales.orders (list_files): lakemon: list_files returned no result") != std::string::npos,
	       "second diagnostic is not truncated");
	Expect(joined.find("; ") != std::string::npos, "multiple diagnostics are joined");
}

int main() {
	TestDefaultLadderIsByteWeighted();
	TestValidateRejectsZeroAndOversizeSteps();
	TestEqualSizeFilesCutThreeThresholds();
	TestLadderNeverEndsAtZero();
	TestNoDeleteDataSkipsRewrite();
	TestHugeLowRatioFileDoesNotHideSmallHotFile();
	TestEqualWidthRatioStepsMisgroupTheSameFiles();
	TestEqualCountSizeSplitsMixCleanAndDirtyFiles();
	TestByteBudgetCapsARung();
	TestFirstFileMayExceedBudgetToMakeProgress();
	TestZeroRecordFileWithDeletesHasRatioOne();
	TestMinDeleteRatioExcludesGiantNearClean();
	TestInvalidLadderFailsClosed();
	TestMaxRewriteStepsIsLadderSize();
	TestZeroRatioSliceIsSkipped();
	TestZeroRatioFileDoesNotPoisonPositiveRung();
	TestDuplicateThresholdMergesFiles();
	TestListFilesSizeRatioCanBeExcluded();
	TestDeletedBytesUsesCount();
	TestMultiRungPlanEmitsDistinctCalls();
	TestCutsUseBytesNotDeleteCount();
	TestMergeTiers();
	TestTableHintMerge();
	TestOneFileDoesNotHintMerge();
	TestPipelineStepsAreIndependent();
	TestGlobalExpireCleanupSkipSemantics();
	TestCumulativeCrossesCutIsOverflowSafe();
	TestFlushInlinedGuardAndDetails();
	TestInlinedOnlyTablesGetHints();
	TestFormatInventoryDiagnosticsJoinsAll();
	if (TestFailures()) {
		std::cerr << TestFailures() << " failure(s)" << std::endl;
		return 1;
	}
	std::cout << "policy tests ok" << std::endl;
	return 0;
}
