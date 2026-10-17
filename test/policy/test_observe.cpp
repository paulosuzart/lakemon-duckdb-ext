#include "expect.hpp"
#include "lakemon_observe.hpp"
#include "lakemon_progress.hpp"

#include <iostream>
#include <string>

static bool Contains(const std::string &haystack, const std::string &needle) {
	return haystack.find(needle) != std::string::npos;
}

static void TestStepWeightPrefersBytesThenFiles() {
	Expect(lakemon::StepWeight(4096, 3) == 4096, "planned bytes win");
	Expect(lakemon::StepWeight(0, 7) == 1, "file count is not mixed into the byte unit");
	Expect(lakemon::StepWeight(0, 0) == 1, "flush / expire / skip still have unit weight");
	Expect(lakemon::StepWeight(0, -1) == 1, "negative file count is treated as unknown");
}

static void TestProgressFractionAndPercent() {
	Expect(lakemon::ProgressFraction(0, 0) == 1.0, "empty plan is complete");
	Expect(lakemon::ProgressFraction(0, 8) == 0.0, "nothing completed");
	Expect(lakemon::ProgressFraction(2, 8) == 0.25, "quarter of the weight");
	Expect(lakemon::ProgressFraction(8, 8) == 1.0, "all weight completed");
	Expect(lakemon::ProgressFraction(9, 8) == 1.0, "overshoot clamps to 1");
	Expect(lakemon::ProgressFraction(1, 4) == 0.25, "first quarter of planned bytes");
	Expect(lakemon::ProgressFraction(100, 500) == 0.2, "first rewrite rung is 20 percent of weight");
}

static void TestByteWeightedProgressMatchesRewriteBias() {
	// A 10 MiB rewrite plus a unit-weight flush should not be 50/50.
	const uint64_t flush = lakemon::StepWeight(0, 0);
	const uint64_t rewrite = lakemon::StepWeight(10ULL * 1024 * 1024, 4);
	const uint64_t total = flush + rewrite;
	Expect(rewrite > flush * 100, "byte-weighted rewrite dominates flush");
	Expect(lakemon::ProgressFraction(flush, total) < 0.01, "flush alone is a tiny fraction");
	Expect(lakemon::ProgressFraction(flush + rewrite, total) == 1.0, "flush + rewrite completes");
}

static void TestCatalogLockErrorHint() {
	Expect(lakemon::LooksLikeCatalogLockWait("ERROR: canceling statement due to lock timeout"),
	       "Postgres lock timeout");
	Expect(lakemon::LooksLikeCatalogLockWait("deadlock detected"), "deadlock");
	Expect(lakemon::LooksLikeCatalogLockWait("could not obtain lock on relation"), "obtain lock");
	Expect(!lakemon::LooksLikeCatalogLockWait("catalog is not attached"), "ordinary errors stay ordinary");
	const std::string annotated = lakemon::AnnotateCatalogLockError("ERROR: lock timeout");
	Expect(Contains(annotated, "another DuckDB session"), "hint names the other session");
	Expect(Contains(annotated, "-clock_timeout=30s"), "hint uses the libpq-safe ATTACH option");
	Expect(Contains(annotated, "ATTACH"), "hint names the ATTACH fallback");
	const std::string again = lakemon::AnnotateCatalogLockError(annotated);
	Expect(again == annotated, "hint is not duplicated");
}

static void TestStepLogLineIsParseable() {
	const std::string start = lakemon::FormatStepLog(true, "rewrite", "rung_1", "main", "events", "planned", 4, 0,
	                                                 1024, 0.25, 0, "");
	Expect(Contains(start, "lakemon start"), "start prefix");
	Expect(Contains(start, "step=rewrite"), "step field");
	Expect(Contains(start, "action=rung_1"), "action field");
	Expect(Contains(start, "table=main.events"), "qualified table");
	Expect(Contains(start, "files_processed=4"), "files processed");
	Expect(Contains(start, "planned_bytes=1024"), "planned bytes");
	Expect(Contains(start, "progress=0.250"), "progress fraction");
	Expect(!Contains(start, "duration_ms="), "start has no duration");

	const std::string end = lakemon::FormatStepLog(false, "merge", "tier_micro", "main", "events", "ok", 8, 2, 4096,
	                                               1.0, 1234, "");
	Expect(Contains(end, "lakemon end"), "end prefix");
	Expect(Contains(end, "status=ok"), "status field");
	Expect(Contains(end, "duration_ms=1234"), "duration field");
	Expect(Contains(end, "files_created=2"), "files created");
	Expect(Contains(end, "progress=1.000"), "completed progress");

	const std::string err = lakemon::FormatStepLog(false, "expire_snapshots", "expire", "", "", "error", 0, 0, 0, 1.0,
	                                               50, "lock timeout");
	Expect(Contains(err, "error=lock timeout"), "error payload");
	Expect(Contains(err, "table="), "global steps still emit table=");
}

int main() {
	TestStepWeightPrefersBytesThenFiles();
	TestProgressFractionAndPercent();
	TestByteWeightedProgressMatchesRewriteBias();
	TestCatalogLockErrorHint();
	TestStepLogLineIsParseable();
	if (TestFailures()) {
		std::cerr << TestFailures() << " failure(s)" << std::endl;
		return 1;
	}
	std::cout << "observe tests ok" << std::endl;
	return 0;
}
