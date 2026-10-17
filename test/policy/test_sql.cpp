#include "expect.hpp"
#include "lakemon_pipeline.hpp"
#include "lakemon_sql.hpp"

#include <iostream>
#include <string>

static bool Contains(const std::string &haystack, const std::string &needle) {
	return haystack.find(needle) != std::string::npos;
}

static void TestInlinedDeleteTableNameIsDeterministic() {
	Expect(lakemon::InlinedDeleteTableName(0) == "ducklake_inlined_delete_0", "table id 0");
	Expect(lakemon::InlinedDeleteTableName(17) == "ducklake_inlined_delete_17", "table id 17");
}

static void TestFlushSchemaQualifiedUsesNamedArgs() {
	const auto ref = lakemon::ParseTableRef("dp_gold.deal_chat_bridge");
	const std::string sql = lakemon::FlushInlinedDataCall("dlw", ref);
	Expect(sql == "CALL ducklake_flush_inlined_data('dlw', table_name => 'deal_chat_bridge', "
	              "schema_name => 'dp_gold')",
	       "schema.table flush uses named table_name and schema_name");
	Expect(!Contains(sql, "('dlw', 'deal_chat_bridge'"),
	       "flush must not emit ducklake_flush_inlined_data(VARCHAR, VARCHAR)");
	Expect(!Contains(sql, "schema =>"), "flush uses schema_name, not schema =>");
}

static void TestFlushCatalogOnlyOmitsTable() {
	lakemon::TableRef ref;
	Expect(ref.table.empty(), "default TableRef has no table");
	Expect(ref.schema == "main", "default TableRef schema is main");
	Expect(lakemon::FlushInlinedDataCall("dlw", ref) == "CALL ducklake_flush_inlined_data('dlw')",
	       "catalog-only flush is a single positional catalog argument");
}

static void TestFlushMainAndEmptySchemaOmitSchemaName() {
	const auto bare = lakemon::ParseTableRef("events");
	Expect(lakemon::FlushInlinedDataCall("dlw", bare) ==
	           "CALL ducklake_flush_inlined_data('dlw', table_name => 'events')",
	       "unqualified table uses named table_name and omits main schema");

	const auto main_qualified = lakemon::ParseTableRef("main.events");
	Expect(lakemon::FlushInlinedDataCall("dlw", main_qualified) ==
	           "CALL ducklake_flush_inlined_data('dlw', table_name => 'events')",
	       "schema main is omitted from flush");

	lakemon::TableRef empty_schema;
	empty_schema.schema.clear();
	empty_schema.table = "events";
	Expect(lakemon::FlushInlinedDataCall("dlw", empty_schema) ==
	           "CALL ducklake_flush_inlined_data('dlw', table_name => 'events')",
	       "empty schema is omitted from flush");
}

static void TestRewriteMergeKeepPositionalTableAndSchema() {
	const auto ref = lakemon::ParseTableRef("dp_gold.deal_chat_bridge");
	Expect(lakemon::TableArg(ref) == ", 'deal_chat_bridge'",
	       "rewrite/merge keep a positional table VARCHAR");
	Expect(lakemon::SchemaNamed(ref) == ", schema => 'dp_gold'",
	       "rewrite/merge keep schema => for non-main schemas");
	Expect(lakemon::SchemaNamed(lakemon::ParseTableRef("events")).empty(),
	       "main schema is omitted for rewrite/merge");
	Expect(lakemon::RewriteDataFilesCall("dlw", ref, 0.05) ==
	           "CALL ducklake_rewrite_data_files('dlw', 'deal_chat_bridge', schema => 'dp_gold', "
	           "delete_threshold => 0.05)",
	       "rewrite CALL keeps positional table plus data-driven threshold");
}

static void TestGlobalRetentionCallsAreCatalogOnly() {
	Expect(lakemon::ExpireSnapshotsCall("lake", "7 days") ==
	           "CALL ducklake_expire_snapshots('lake', older_than => now()::TIMESTAMP - INTERVAL '7 days')",
	       "expire snapshots is catalog-global");
	Expect(lakemon::CleanupOldFilesCall("lake", "3 days") ==
	           "CALL ducklake_cleanup_old_files('lake', older_than => now()::TIMESTAMP - INTERVAL '3 days')",
	       "cleanup uses older_than when set");
	Expect(lakemon::CleanupOldFilesCall("lake", "") == "CALL ducklake_cleanup_old_files('lake', cleanup_all => true)",
	       "cleanup_all when delete interval unset");
	Expect(lakemon::DeleteOrphanedFilesCall("lake") == "CALL ducklake_delete_orphaned_files('lake')",
	       "orphan delete is catalog-global");
	Expect(!Contains(lakemon::ExpireSnapshotsCall("lake", "7 days"), "table"), "expire has no table argument");
	Expect(!Contains(lakemon::FlushInlinedDataCall("lake", lakemon::ParseTableRef("s.t")), "expire"),
	       "table flush CALL does not expire snapshots");
}

static void TestExpireIntervalBindsWithoutIcu() {
	const std::string expire = lakemon::ExpireSnapshotsCall("lake", "7 days");
	const std::string cleanup = lakemon::CleanupOldFilesCall("lake", "3 days");
	Expect(Contains(expire, "now()::TIMESTAMP - INTERVAL '7 days'"),
	       "expire older_than casts now() to TIMESTAMP");
	Expect(Contains(cleanup, "now()::TIMESTAMP - INTERVAL '3 days'"),
	       "cleanup older_than casts now() to TIMESTAMP");
	Expect(!Contains(expire, "older_than => now() - INTERVAL"),
	       "expire must not emit uncast now() - INTERVAL (needs ICU)");
	Expect(!Contains(cleanup, "older_than => now() - INTERVAL"),
	       "cleanup must not emit uncast now() - INTERVAL (needs ICU)");
	Expect(lakemon::TimestampMinusInterval("7 days") == "now()::TIMESTAMP - INTERVAL '7 days'",
	       "TimestampMinusInterval is the ICU-free older_than fragment");
}

static void TestExecuteGuardsRefuseBindOnlyAndBegin() {
	Expect(lakemon::LeadingSQLKeyword("CALL lakemon_maintain('lake')") == "CALL", "plain CALL");
	Expect(lakemon::LeadingSQLKeyword("  SELECT * FROM lakemon_maintain('lake')") == "SELECT", "SELECT");
	Expect(lakemon::LeadingSQLKeyword("-- comment\nEXPLAIN CALL lakemon_maintain('lake')") == "EXPLAIN",
	       "skips line comments");
	Expect(lakemon::LeadingSQLKeyword("/* x */ (PREPARE p AS SELECT 1)") == "PREPARE", "skips block comments and parens");
	Expect(lakemon::IsMaintainExecuteKeyword("CALL"), "CALL may execute");
	Expect(lakemon::IsMaintainExecuteKeyword("SELECT"), "SELECT may execute");
	Expect(lakemon::IsMaintainExecuteKeyword("FROM"), "FROM may execute");
	Expect(lakemon::IsMaintainExecuteKeyword("WITH"), "WITH may execute");
	Expect(lakemon::IsMaintainExecuteKeyword("PRAGMA"), "PRAGMA may execute");
	Expect(!lakemon::IsMaintainExecuteKeyword("EXPLAIN"), "EXPLAIN is not an execute form");
	Expect(!lakemon::IsMaintainExecuteKeyword("PREPARE"), "PREPARE is not an execute form");
	Expect(!lakemon::IsMaintainExecuteKeyword("EXECUTE"), "EXECUTE is not an execute form");
	Expect(!lakemon::IsMaintainExecuteKeyword("DESCRIBE"), "DESCRIBE is not an execute form");
	Expect(!lakemon::IsMaintainExecuteKeyword("SUMMARIZE"), "SUMMARIZE is not an execute form");
	Expect(!lakemon::IsMaintainExecuteKeyword("CREATE"), "CREATE VIEW is not an execute form");

	std::string error;
	Expect(!lakemon::MaintainExecuteRefused(true, "CALL lakemon_maintain('lake')", error),
	       "autocommit CALL is allowed");
	Expect(lakemon::MaintainExecuteRefused(false, "CALL lakemon_maintain('lake')", error),
	       "explicit BEGIN is refused");
	Expect(Contains(error, "explicit transaction"), "BEGIN error names the transaction");
	Expect(Contains(error, "BEGIN"), "BEGIN error mentions BEGIN");
	Expect(lakemon::MaintainExecuteRefused(true, "EXPLAIN SELECT * FROM lakemon_maintain('lake')", error),
	       "EXPLAIN is refused");
	Expect(Contains(error, "EXPLAIN"), "EXPLAIN error names the keyword");
	Expect(lakemon::MaintainExecuteRefused(true, "PREPARE p AS SELECT * FROM lakemon_maintain('lake')", error),
	       "PREPARE is refused");
	Expect(Contains(error, "PREPARE"), "PREPARE error names the keyword");
	Expect(lakemon::MaintainExecuteRefused(true, "EXECUTE p", error), "EXECUTE is refused");
	Expect(lakemon::MaintainExecuteRefused(true, "CREATE VIEW v AS SELECT * FROM lakemon_maintain('lake')", error),
	       "CREATE VIEW is refused");
	Expect(Contains(error, "CREATE"), "CREATE VIEW error names the keyword");
	Expect(!lakemon::MaintainExecuteRefused(true, "PRAGMA", error),
	       "PRAGMA keyword is allowed (pragma query has no active_query)");
	Expect(!lakemon::MaintainExecuteRefused(true, "PRAGMA lakemon_maintain('lake')", error),
	       "autocommit PRAGMA is allowed");
	Expect(lakemon::MaintainExecuteRefused(false, "PRAGMA lakemon_maintain('lake')", error),
	       "PRAGMA inside BEGIN is refused");
	Expect(!lakemon::MaintainExecuteRefused(true, "SELECT * FROM lakemon_maintain('lake', dry_run => false)", error),
	       "plain SELECT is allowed");
	Expect(lakemon::MaintainExecuteRefused(true, "SELECT * FROM lakemon_maintain(?, dry_run => false)", error, true),
	       "prepared parameters are refused");
	Expect(Contains(error, "prepared"), "parameter error names prepared statements");
}

static void TestRewriteCallNeverUsesZeroThreshold() {
	lakemon::TableRef ref;
	ref.schema = "main";
	ref.table = "events";
	Expect(lakemon::ShouldEmitRewriteCall(0.05), "positive threshold may emit");
	Expect(!lakemon::ShouldEmitRewriteCall(0.0), "lakemon must not emit delete_threshold 0");
	Expect(!lakemon::ShouldEmitRewriteCall(0), "integer 0 must not emit");
	const std::string sql = lakemon::RewriteDataFilesCall("lake", ref, 0.05);
	Expect(Contains(sql, "delete_threshold => 0.05"), "positive CALL keeps the derived threshold");
	Expect(!Contains(sql, "delete_threshold => 0)"), "generated SQL is not a zero-threshold CALL");
}

int main() {
	TestInlinedDeleteTableNameIsDeterministic();
	TestFlushSchemaQualifiedUsesNamedArgs();
	TestFlushCatalogOnlyOmitsTable();
	TestFlushMainAndEmptySchemaOmitSchemaName();
	TestRewriteMergeKeepPositionalTableAndSchema();
	TestGlobalRetentionCallsAreCatalogOnly();
	TestExpireIntervalBindsWithoutIcu();
	TestExecuteGuardsRefuseBindOnlyAndBegin();
	TestRewriteCallNeverUsesZeroThreshold();
	if (TestFailures()) {
		std::cerr << TestFailures() << " failure(s)" << std::endl;
		return 1;
	}
	std::cout << "sql tests ok" << std::endl;
	return 0;
}
