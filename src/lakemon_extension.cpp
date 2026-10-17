#define DUCKDB_EXTENSION_MAIN

#include "lakemon_extension.hpp"
#include "lakemon_catalog.hpp"
#include "lakemon_compat.hpp"
#include "lakemon_maintain.hpp"
#include "lakemon_policy.hpp"
#include "lakemon_progress.hpp"
#include "lakemon_store.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"

#include <atomic>
#include <exception>
#include <new>
#include <vector>

namespace duckdb {

namespace {

struct MaintainBindData : public TableFunctionData {
	lakemon::MaintainOptions options;
};

struct GlobalMaintainBindData : public TableFunctionData {
	lakemon::GlobalMaintainOptions options;
};

struct StatsBindData : public TableFunctionData {
	string catalog;
	lakemon::TableRef table;
};

struct PolicyBindData : public TableFunctionData {
	string catalog;
};

struct SetPolicyBindData : public TableFunctionData {
	string catalog;
	string kind;
	string name;
	lakemon::policy::PolicyFieldPatch patch;
	bool reset_all = false;
};

struct RowState : public GlobalTableFunctionState {
	vector<vector<Value>> rows;
	vector<uint64_t> weights;
	std::atomic<idx_t> offset {0};
	uint64_t total_weight = 0;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static void EmitRows(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	if (!input.global_state) {
		lakemon::FinishChunk(output, 0);
		return;
	}
	auto &state = input.global_state->Cast<RowState>();
	idx_t count = 0;
	const idx_t out_cols = output.ColumnCount();
	idx_t pos = state.offset.load();
	while (pos < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[pos++];
		const idx_t row_cols = static_cast<idx_t>(row.size());
		const idx_t use_cols = row_cols < out_cols ? row_cols : out_cols;
		for (idx_t col = 0; col < use_cols; col++) {
			lakemon::WriteChunkValue(output, col, count, row[col]);
		}
		count++;
	}
	state.offset.store(pos);
	lakemon::FinishChunk(output, count);
}

// Negative = unknown. CALL / SELECT execute in bind_replace, so this path
// only emits already-finished rows and would jump 0 → 100. Stderr step
// lines carry the byte-weighted fraction instead.
static double MaintainScanProgress(ClientContext &, const FunctionData *,
                                   const GlobalTableFunctionState *) {
	return -1;
}

static string RequireCatalog(const Value &value) {
	if (value.IsNull()) {
		throw InvalidInputException("lakemon: catalog name is required");
	}
	const string catalog = StringValue::Get(value);
	if (catalog.empty()) {
		throw InvalidInputException("lakemon: catalog name is required");
	}
	return catalog;
}

template <typename NamedMap>
static void ParseNamedMaintainMap(const NamedMap &named, lakemon::MaintainOptions &options) {
	for (auto &entry : named) {
		if (entry.second.IsNull()) {
			throw InvalidInputException("lakemon: named parameter cannot be NULL");
		}
		if (entry.first == "dry_run") {
			options.dry_run = BooleanValue::Get(entry.second);
		} else if (entry.first == "max_compacted_files") {
			// BigIntValue::Get avoids Value::GetValue<T> in a dependent context (C++11).
			options.max_compacted_files = BigIntValue::Get(entry.second);
		}
	}
}

template <typename NamedMap>
static void ParseNamedGlobalMaintainMap(const NamedMap &named, lakemon::GlobalMaintainOptions &options) {
	for (auto &entry : named) {
		if (entry.second.IsNull()) {
			throw InvalidInputException("lakemon: named parameter cannot be NULL");
		}
		if (entry.first == "dry_run") {
			options.dry_run = BooleanValue::Get(entry.second);
		} else if (entry.first == "skip_expire") {
			options.skip_expire = BooleanValue::Get(entry.second);
		} else if (entry.first == "skip_cleanup") {
			options.skip_cleanup = BooleanValue::Get(entry.second);
		} else if (entry.first == "expire_older_than") {
			options.expire_older_than = StringValue::Get(entry.second);
			options.expire_older_than_set = true;
		} else if (entry.first == "delete_older_than") {
			options.delete_older_than = StringValue::Get(entry.second);
			options.delete_older_than_set = true;
		}
	}
}

static void ParseNamedMaintain(TableFunctionBindInput &input, lakemon::MaintainOptions &options) {
	ParseNamedMaintainMap(input.named_parameters, options);
}

static void ParseNamedGlobalMaintain(TableFunctionBindInput &input, lakemon::GlobalMaintainOptions &options) {
	ParseNamedGlobalMaintainMap(input.named_parameters, options);
}

static bool HasPreparedParameters(TableFunctionBindInput &input) {
	if (!input.binder) {
		return false;
	}
	auto param_map = input.binder->GetParameters();
	if (!param_map) {
		return false;
	}
	return !param_map->GetParameters().empty();
}

static void MaintainReturnTypes(vector<LogicalType> &return_types, lakemon::ColumnNameList &names) {
	names = {"step", "schema_name", "table_name", "action", "status", "files_processed", "files_created", "details"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::BIGINT,  LogicalType::BIGINT,  LogicalType::VARCHAR};
}

static void FillMaintainRows(RowState &state, const std::vector<lakemon::MaintainRow> &rows) {
	for (auto &row : rows) {
		state.rows.push_back({Value(row.step), Value(row.schema_name), Value(row.table_name), Value(row.action),
		                       Value(row.status), Value::BIGINT(row.files_processed), Value::BIGINT(row.files_created),
		                       Value(row.details)});
		const uint64_t weight = lakemon::StepWeight(row.planned_bytes, row.files_processed);
		state.weights.push_back(weight);
		state.total_weight += weight;
	}
}

static lakemon::MaintainRow OrchestrateErrorRow(const char *step, const char *details) {
	lakemon::MaintainRow row;
	row.step = step;
	row.action = "orchestrate";
	row.status = "error";
	row.details = details;
	return row;
}

static unique_ptr<TableRef> ParseSelectAsSubquery(ClientContext &context, const string &sql) {
#if LAKEMON_DUCKDB_2
	Parser parser(context);
#else
	(void)context;
	Parser parser;
#endif
	parser.ParseQuery(sql);
	if (parser.statements.size() != 1 || !parser.statements[0] ||
	    parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw InvalidInputException("lakemon: internal execute plan must be a single SELECT");
	}
	auto select = unique_ptr<SelectStatement>(static_cast<SelectStatement *>(parser.statements[0].release()));
	return make_uniq<SubqueryRef>(std::move(select));
}

static unique_ptr<TableRef> MaintainBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	lakemon::MaintainOptions options;
	try {
		ParseNamedMaintain(input, options);
	} catch (const InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const Exception &) {
		throw;
	} catch (const std::exception &ex) {
		throw InvalidInputException("lakemon: %s", lakemon::SafeWhat(ex));
	}

	if (!options.dry_run) {
		lakemon::RequireMaintainExecute(context, HasPreparedParameters(input));
	}

	options.catalog = RequireCatalog(input.inputs[0]);
	if (input.inputs.size() >= 2 && !input.inputs[1].IsNull()) {
		options.table = lakemon::ParseTableRef(StringValue::Get(input.inputs[1]));
	}

	std::vector<lakemon::MaintainRow> rows;
	try {
		rows = lakemon::RunMaintain(context, options);
	} catch (const InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const Exception &ex) {
		rows.push_back(OrchestrateErrorRow("maintain", lakemon::SafeWhat(ex)));
	} catch (const std::exception &ex) {
		rows.push_back(OrchestrateErrorRow("maintain", lakemon::SafeWhat(ex)));
	}

	if (!options.dry_run) {
		lakemon::ExecutePlannedRows(context, rows);
	}
	auto ref = ParseSelectAsSubquery(context, lakemon::BindReplaceQuery(rows, false));
	ref->alias = input.ref.alias;
	ref->column_name_alias = input.ref.column_name_alias;
	return ref;
}

static unique_ptr<TableRef> GlobalMaintainBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	lakemon::GlobalMaintainOptions options;
	try {
		ParseNamedGlobalMaintain(input, options);
	} catch (const InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const Exception &) {
		throw;
	} catch (const std::exception &ex) {
		throw InvalidInputException("lakemon: %s", lakemon::SafeWhat(ex));
	}

	if (!options.dry_run) {
		lakemon::RequireMaintainExecute(context, HasPreparedParameters(input));
	}

	options.catalog = RequireCatalog(input.inputs[0]);

	std::vector<lakemon::MaintainRow> rows;
	try {
		rows = lakemon::RunGlobalMaintain(context, options);
	} catch (const InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const Exception &ex) {
		rows.push_back(OrchestrateErrorRow("maintain_global", lakemon::SafeWhat(ex)));
	} catch (const std::exception &ex) {
		rows.push_back(OrchestrateErrorRow("maintain_global", lakemon::SafeWhat(ex)));
	}

	if (!options.dry_run) {
		lakemon::ExecutePlannedRows(context, rows);
	}
	auto ref = ParseSelectAsSubquery(context, lakemon::BindReplaceQuery(rows, false));
	ref->alias = input.ref.alias;
	ref->column_name_alias = input.ref.column_name_alias;
	return ref;
}

static unique_ptr<FunctionData> MaintainBind(ClientContext &, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, lakemon::ColumnNameList &names) {
	auto data = make_uniq<MaintainBindData>();
	data->options.catalog = RequireCatalog(input.inputs[0]);
	if (input.inputs.size() >= 2 && !input.inputs[1].IsNull()) {
		data->options.table = lakemon::ParseTableRef(StringValue::Get(input.inputs[1]));
	}
	try {
		ParseNamedMaintain(input, data->options);
	} catch (const InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const Exception &) {
		throw;
	} catch (const std::exception &ex) {
		throw InvalidInputException("lakemon: %s", lakemon::SafeWhat(ex));
	}

	MaintainReturnTypes(return_types, names);
	return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> MaintainInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<MaintainBindData>();
	auto state = make_uniq<RowState>();
	std::vector<lakemon::MaintainRow> rows;
	try {
		rows = lakemon::RunMaintain(context, bind.options);
	} catch (const InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const Exception &ex) {
		rows.push_back(OrchestrateErrorRow("maintain", lakemon::SafeWhat(ex)));
	} catch (const std::exception &ex) {
		rows.push_back(OrchestrateErrorRow("maintain", lakemon::SafeWhat(ex)));
	}
	FillMaintainRows(*state, rows);
	return std::move(state);
}

static unique_ptr<FunctionData> GlobalMaintainBind(ClientContext &, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, lakemon::ColumnNameList &names) {
	auto data = make_uniq<GlobalMaintainBindData>();
	data->options.catalog = RequireCatalog(input.inputs[0]);
	try {
		ParseNamedGlobalMaintain(input, data->options);
	} catch (const InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const Exception &) {
		throw;
	} catch (const std::exception &ex) {
		throw InvalidInputException("lakemon: %s", lakemon::SafeWhat(ex));
	}

	MaintainReturnTypes(return_types, names);
	return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> GlobalMaintainInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<GlobalMaintainBindData>();
	auto state = make_uniq<RowState>();
	std::vector<lakemon::MaintainRow> rows;
	try {
		rows = lakemon::RunGlobalMaintain(context, bind.options);
	} catch (const InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const Exception &ex) {
		rows.push_back(OrchestrateErrorRow("maintain_global", lakemon::SafeWhat(ex)));
	} catch (const std::exception &ex) {
		rows.push_back(OrchestrateErrorRow("maintain_global", lakemon::SafeWhat(ex)));
	}
	FillMaintainRows(*state, rows);
	return std::move(state);
}

static string MaintainPragmaQuery(ClientContext &context, const FunctionParameters &parameters) {
	lakemon::MaintainOptions options;
	options.catalog = RequireCatalog(parameters.values[0]);
	if (parameters.values.size() >= 2 && !parameters.values[1].IsNull()) {
		options.table = lakemon::ParseTableRef(StringValue::Get(parameters.values[1]));
	}
	ParseNamedMaintainMap(parameters.named_parameters, options);
	try {
		if (!options.dry_run) {
			lakemon::RequireMaintainExecute(context, false, "PRAGMA");
		}
		auto rows = lakemon::RunMaintain(context, options);
		if (!options.dry_run) {
			lakemon::ExecutePlannedRows(context, rows);
		}
		return lakemon::BindReplaceQuery(rows, false);
	} catch (const InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const Exception &) {
		throw;
	} catch (const std::exception &ex) {
		throw InvalidInputException("lakemon: %s", lakemon::SafeWhat(ex));
	}
}

static string GlobalMaintainPragmaQuery(ClientContext &context, const FunctionParameters &parameters) {
	lakemon::GlobalMaintainOptions options;
	options.catalog = RequireCatalog(parameters.values[0]);
	ParseNamedGlobalMaintainMap(parameters.named_parameters, options);
	try {
		if (!options.dry_run) {
			lakemon::RequireMaintainExecute(context, false, "PRAGMA");
		}
		auto rows = lakemon::RunGlobalMaintain(context, options);
		if (!options.dry_run) {
			lakemon::ExecutePlannedRows(context, rows);
		}
		return lakemon::BindReplaceQuery(rows, false);
	} catch (const InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const Exception &) {
		throw;
	} catch (const std::exception &ex) {
		throw InvalidInputException("lakemon: %s", lakemon::SafeWhat(ex));
	}
}

static unique_ptr<FunctionData> StatsBind(ClientContext &, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, lakemon::ColumnNameList &names) {
	auto data = make_uniq<StatsBindData>();
	data->catalog = RequireCatalog(input.inputs[0]);
	if (input.inputs.size() >= 2 && !input.inputs[1].IsNull()) {
		data->table = lakemon::ParseTableRef(StringValue::Get(input.inputs[1]));
	}
	names = {"schema_name",
	         "table_name",
	         "file_count",
	         "file_size_bytes",
	         "delete_file_count",
	         "delete_file_size_bytes",
	         "delete_count",
	         "deleted_bytes_weighted",
	         "delete_ratio",
	         "rewrite_plan",
	         "rewrite_threshold",
	         "merge_tier_hint",
	         "target_file_size",
	         "auto_compact"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT,
	                LogicalType::BIGINT,  LogicalType::BIGINT,  LogicalType::BIGINT, LogicalType::BIGINT,
	                LogicalType::DOUBLE,  LogicalType::VARCHAR, LogicalType::DOUBLE, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::BOOLEAN};
	return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> StatsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<StatsBindData>();
	auto state = make_uniq<RowState>();
	std::vector<lakemon::policy::TableHint> hints;
	try {
		hints = lakemon::InventoryTables(context, bind.catalog, bind.table);
	} catch (const InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const Exception &) {
		throw;
	} catch (const std::exception &ex) {
		throw InvalidInputException("lakemon: %s", lakemon::SafeWhat(ex));
	}
	for (auto &hint : hints) {
		state->rows.push_back({Value(hint.schema_name), Value(hint.table_name), Value::BIGINT(static_cast<int64_t>(hint.file_count)),
		                       Value::BIGINT(static_cast<int64_t>(hint.file_size_bytes)),
		                       Value::BIGINT(static_cast<int64_t>(hint.delete_file_count)),
		                       Value::BIGINT(static_cast<int64_t>(hint.delete_file_size_bytes)),
		                       Value::BIGINT(static_cast<int64_t>(hint.delete_count)),
		                       Value::BIGINT(static_cast<int64_t>(hint.deleted_bytes_weighted)), Value(hint.delete_ratio),
		                       Value(hint.rewrite_plan), Value(hint.rewrite_threshold), Value(hint.merge_tier_hint),
		                       Value(hint.target_file_size), Value::BOOLEAN(hint.auto_compact)});
	}
	return std::move(state);
}

static void PolicyReturnTypes(vector<LogicalType> &return_types, lakemon::ColumnNameList &names) {
	names = {"kind", "name", "min_value", "max_value", "threshold_or_target", "notes", "source"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
}

static void SetPolicyReturnTypes(vector<LogicalType> &return_types, lakemon::ColumnNameList &names) {
	names = {"kind", "name", "status", "details"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
}

static void EmitPolicy(RowState &state, const lakemon::policy::ActivePolicy &policy) {
	const char *source =
	    lakemon::policy::PolicyKeyOverridden(policy, lakemon::policy::kKindRewriteLadder, lakemon::policy::kLadderName)
	        ? lakemon::policy::kSourceOverride
	        : lakemon::policy::kSourceDefault;
	state.rows.push_back({Value(lakemon::policy::kKindRewriteLadder), Value(lakemon::policy::kLadderName),
	                      Value("1"), Value(std::to_string(policy.rewrite.max_rewrite_steps)), Value("data-driven"),
	                      Value(lakemon::policy::FormatRewriteLadderNotes(policy.rewrite)), Value(source)});
	for (const auto &tier : policy.tiers) {
		const char *source = lakemon::policy::PolicyKeyOverridden(policy, lakemon::policy::kKindMergeTier, tier.name)
		                         ? lakemon::policy::kSourceOverride
		                         : lakemon::policy::kSourceDefault;
		state.rows.push_back({Value(lakemon::policy::kKindMergeTier), Value(tier.name),
		                      Value(std::to_string(tier.min_file_size)), Value(std::to_string(tier.max_file_size)),
		                      Value(tier.target_file_size),
		                      Value("max_compacted_files=" + std::to_string(tier.max_compacted_files)), Value(source)});
	}
}

static unique_ptr<FunctionData> PolicyBind(ClientContext &, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, lakemon::ColumnNameList &names) {
	auto data = make_uniq<PolicyBindData>();
	if (!input.inputs.empty()) {
		data->catalog = RequireCatalog(input.inputs[0]);
	}
	PolicyReturnTypes(return_types, names);
	return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> PolicyInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<PolicyBindData>();
	auto state = make_uniq<RowState>();
	if (bind.catalog.empty()) {
		EmitPolicy(*state, lakemon::policy::DefaultPolicy());
		return std::move(state);
	}
	try {
		std::string error;
		const auto policy = lakemon::LoadActivePolicy(context, bind.catalog, &error);
		EmitPolicy(*state, policy);
		if (!error.empty()) {
			state->rows.push_back({Value("error"), Value(""), Value(""), Value(""), Value(""), Value(error),
			                       Value("error")});
		}
	} catch (const InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const Exception &) {
		throw;
	} catch (const std::exception &ex) {
		throw InvalidInputException("lakemon: %s", lakemon::SafeWhat(ex));
	}
	return std::move(state);
}

static bool AsNonNegative(int64_t value, uint64_t &out, const char *field) {
	if (value < 0) {
		throw InvalidInputException("lakemon: %s must be >= 0", field);
	}
	out = static_cast<uint64_t>(value);
	return true;
}

static void ParseNamedSetPolicy(TableFunctionBindInput &input, SetPolicyBindData &data) {
	for (auto &entry : input.named_parameters) {
		if (entry.second.IsNull()) {
			throw InvalidInputException("lakemon: named parameter cannot be NULL");
		}
		if (entry.first == "reset") {
			data.patch.reset = BooleanValue::Get(entry.second);
		} else if (entry.first == "reset_all") {
			data.reset_all = BooleanValue::Get(entry.second);
		} else if (entry.first == "byte_budget") {
			data.patch.set_byte_budget =
			    AsNonNegative(entry.second.GetValue<int64_t>(), data.patch.byte_budget, "byte_budget");
		} else if (entry.first == "max_rewrite_steps") {
			data.patch.set_max_rewrite_steps =
			    AsNonNegative(entry.second.GetValue<int64_t>(), data.patch.max_rewrite_steps, "max_rewrite_steps");
		} else if (entry.first == "min_delete_ratio") {
			data.patch.set_min_delete_ratio = true;
			data.patch.min_delete_ratio = entry.second.GetValue<double>();
		} else if (entry.first == "min_file_size") {
			data.patch.set_min_file_size =
			    AsNonNegative(entry.second.GetValue<int64_t>(), data.patch.min_file_size, "min_file_size");
		} else if (entry.first == "max_file_size") {
			data.patch.set_max_file_size =
			    AsNonNegative(entry.second.GetValue<int64_t>(), data.patch.max_file_size, "max_file_size");
		} else if (entry.first == "target_file_size") {
			data.patch.set_target_file_size = true;
			data.patch.target_file_size = StringValue::Get(entry.second);
		} else if (entry.first == "max_compacted_files") {
			data.patch.set_max_compacted_files =
			    AsNonNegative(entry.second.GetValue<int64_t>(), data.patch.max_compacted_files, "max_compacted_files");
		} else {
			throw InvalidInputException("lakemon: unknown set_policy parameter '%s'", entry.first.c_str());
		}
	}
}

static unique_ptr<FunctionData> SetPolicyBind(ClientContext &, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, lakemon::ColumnNameList &names) {
	auto data = make_uniq<SetPolicyBindData>();
	data->catalog = RequireCatalog(input.inputs[0]);
	if (input.inputs.size() >= 3) {
		if (input.inputs[1].IsNull() || input.inputs[2].IsNull()) {
			throw InvalidInputException("lakemon: kind and name are required");
		}
		data->kind = StringValue::Get(input.inputs[1]);
		data->name = StringValue::Get(input.inputs[2]);
	}
	try {
		ParseNamedSetPolicy(input, *data);
	} catch (const InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const Exception &) {
		throw;
	} catch (const std::exception &ex) {
		throw InvalidInputException("lakemon: %s", lakemon::SafeWhat(ex));
	}
	if (input.inputs.size() < 3 && !data->reset_all) {
		throw InvalidInputException("lakemon: lakemon_set_policy(catalog) requires reset_all => true");
	}
	SetPolicyReturnTypes(return_types, names);
	return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> SetPolicyInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<SetPolicyBindData>();
	auto state = make_uniq<RowState>();
	try {
		if (bind.reset_all) {
			lakemon::DeleteStoredPolicyCatalog(context, bind.catalog);
			state->rows.push_back({Value(""), Value(""), Value("ok"), Value("reset all persisted overrides")});
			return std::move(state);
		}
		std::string error;
		lakemon::policy::ActivePolicy policy = lakemon::LoadActivePolicy(context, bind.catalog, &error);
		if (!error.empty()) {
			throw InvalidInputException("%s", error);
		}
		if (!lakemon::policy::ApplyPolicyPatch(policy, bind.kind, bind.name, bind.patch, error)) {
			throw InvalidInputException("%s", error);
		}
		std::string canonical;
		if (!lakemon::policy::NormalizePolicyKind(bind.kind, canonical, error)) {
			throw InvalidInputException("%s", error);
		}
		if (bind.patch.reset) {
			std::string stored_name = bind.name;
			if (canonical == lakemon::policy::kKindRewriteLadder) {
				stored_name = lakemon::policy::kLadderName;
			} else {
				const auto *tier = lakemon::policy::FindMergeTier(policy, bind.name);
				if (tier) {
					stored_name = tier->name;
				}
			}
			lakemon::DeleteStoredPolicyRow(context, bind.catalog, canonical, stored_name);
			state->rows.push_back(
			    {Value(canonical), Value(stored_name), Value("ok"), Value("reset to built-in default")});
			return std::move(state);
		}
		if (canonical == lakemon::policy::kKindRewriteLadder) {
			lakemon::UpsertStoredPolicyRow(context, bind.catalog, lakemon::policy::RowFromLadder(policy.rewrite));
			state->rows.push_back({Value(canonical), Value(lakemon::policy::kLadderName), Value("ok"),
			                       Value(lakemon::policy::FormatRewriteLadderNotes(policy.rewrite))});
		} else {
			const auto *tier = lakemon::policy::FindMergeTier(policy, bind.name);
			if (!tier) {
				throw InvalidInputException("lakemon: unknown merge tier '%s'", bind.name);
			}
			lakemon::UpsertStoredPolicyRow(context, bind.catalog, lakemon::policy::RowFromTier(*tier));
			state->rows.push_back({Value(canonical), Value(tier->name), Value("ok"),
			                       Value("target_file_size=" + tier->target_file_size)});
		}
	} catch (const InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const Exception &ex) {
		state->rows.push_back({Value(bind.kind), Value(bind.name), Value("error"), Value(lakemon::SafeWhat(ex))});
	} catch (const std::exception &ex) {
		state->rows.push_back({Value(bind.kind), Value(bind.name), Value("error"), Value(lakemon::SafeWhat(ex))});
	}
	return std::move(state);
}

static void VersionFun(DataChunk &, ExpressionState &, Vector &result) {
	lakemon::ReferenceScalar(result, Value("0.1.1"));
}

template <typename FunctionT>
static void AddNamedMaintainParams(FunctionT &function) {
	lakemon::AddNamedParameters(function, {
	    {"dry_run", LogicalType::BOOLEAN},
	    {"max_compacted_files", LogicalType::BIGINT},
	});
}

template <typename FunctionT>
static void AddNamedGlobalMaintainParams(FunctionT &function) {
	lakemon::AddNamedParameters(function, {
	    {"dry_run", LogicalType::BOOLEAN},
	    {"skip_expire", LogicalType::BOOLEAN},
	    {"skip_cleanup", LogicalType::BOOLEAN},
	    {"expire_older_than", LogicalType::VARCHAR},
	    {"delete_older_than", LogicalType::VARCHAR},
	});
}

static void AddNamedSetPolicyParams(TableFunction &function) {
	lakemon::AddNamedParameters(function, {
	    {"reset", LogicalType::BOOLEAN},
	    {"reset_all", LogicalType::BOOLEAN},
	    {"byte_budget", LogicalType::BIGINT},
	    {"max_rewrite_steps", LogicalType::BIGINT},
	    {"min_delete_ratio", LogicalType::DOUBLE},
	    {"min_file_size", LogicalType::BIGINT},
	    {"max_file_size", LogicalType::BIGINT},
	    {"target_file_size", LogicalType::VARCHAR},
	    {"max_compacted_files", LogicalType::BIGINT},
	});
}

} // namespace

void LoadInternal(ExtensionLoader &loader) {
	lakemon::RegisterLakemonSettings(loader.GetDatabaseInstance());
	lakemon::RegisterLakemonLogType(loader.GetDatabaseInstance());

	auto version = ScalarFunction("lakemon_version", {}, LogicalType::VARCHAR, VersionFun);
	loader.RegisterFunction(version);

	TableFunctionSet maintain("lakemon_maintain");
	TableFunction maintain_one({LogicalType::VARCHAR}, EmitRows, MaintainBind, MaintainInit);
	maintain_one.bind_replace = MaintainBindReplace;
	maintain_one.table_scan_progress = MaintainScanProgress;
	AddNamedMaintainParams(maintain_one);
	maintain.AddFunction(maintain_one);
	TableFunction maintain_two({LogicalType::VARCHAR, LogicalType::VARCHAR}, EmitRows, MaintainBind, MaintainInit);
	maintain_two.bind_replace = MaintainBindReplace;
	maintain_two.table_scan_progress = MaintainScanProgress;
	AddNamedMaintainParams(maintain_two);
	maintain.AddFunction(maintain_two);
	loader.RegisterFunction(maintain);

	TableFunctionSet maintain_global("lakemon_maintain_global");
	TableFunction maintain_global_one({LogicalType::VARCHAR}, EmitRows, GlobalMaintainBind, GlobalMaintainInit);
	maintain_global_one.bind_replace = GlobalMaintainBindReplace;
	maintain_global_one.table_scan_progress = MaintainScanProgress;
	AddNamedGlobalMaintainParams(maintain_global_one);
	maintain_global.AddFunction(maintain_global_one);
	loader.RegisterFunction(maintain_global);

	TableFunctionSet stats("lakemon_table_stats");
	stats.AddFunction(TableFunction({LogicalType::VARCHAR}, EmitRows, StatsBind, StatsInit));
	stats.AddFunction(TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, EmitRows, StatsBind, StatsInit));
	loader.RegisterFunction(stats);

	TableFunctionSet policy("lakemon_policy");
	policy.AddFunction(TableFunction({}, EmitRows, PolicyBind, PolicyInit));
	policy.AddFunction(TableFunction({LogicalType::VARCHAR}, EmitRows, PolicyBind, PolicyInit));
	loader.RegisterFunction(policy);

	TableFunctionSet set_policy("lakemon_set_policy");
	TableFunction set_one({LogicalType::VARCHAR}, EmitRows, SetPolicyBind, SetPolicyInit);
	AddNamedSetPolicyParams(set_one);
	set_policy.AddFunction(set_one);
	TableFunction set_three({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, EmitRows,
	                        SetPolicyBind, SetPolicyInit);
	AddNamedSetPolicyParams(set_three);
	set_policy.AddFunction(set_three);
	loader.RegisterFunction(set_policy);

	// Optional PRAGMA form: same plan + ExecutePlannedRows path as CALL.
	// CALL / SELECT use bind_replace: plan, then ExecutePlannedRows (one
	// committed DuckLake CALL each). Not table-function init. dry_run => false
	// is allowed only for CALL / SELECT / FROM / WITH / PRAGMA in autocommit,
	// and is refused when the binder has prepared-statement parameters.
	PragmaFunctionSet maintain_pragma("lakemon_maintain");
	auto maintain_pragma_one =
	    PragmaFunction::PragmaCall("lakemon_maintain", MaintainPragmaQuery, {LogicalType::VARCHAR});
	AddNamedMaintainParams(maintain_pragma_one);
	maintain_pragma.AddFunction(maintain_pragma_one);
	auto maintain_pragma_two = PragmaFunction::PragmaCall("lakemon_maintain", MaintainPragmaQuery,
	                                                     {LogicalType::VARCHAR, LogicalType::VARCHAR});
	AddNamedMaintainParams(maintain_pragma_two);
	maintain_pragma.AddFunction(maintain_pragma_two);
	loader.RegisterFunction(maintain_pragma);

	PragmaFunctionSet maintain_global_pragma("lakemon_maintain_global");
	auto maintain_global_pragma_one =
	    PragmaFunction::PragmaCall("lakemon_maintain_global", GlobalMaintainPragmaQuery, {LogicalType::VARCHAR});
	AddNamedGlobalMaintainParams(maintain_global_pragma_one);
	maintain_global_pragma.AddFunction(maintain_global_pragma_one);
	loader.RegisterFunction(maintain_global_pragma);
}

void LakemonExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string LakemonExtension::Name() {
	return "lakemon";
}

std::string LakemonExtension::Version() const {
#ifdef EXT_VERSION_LAKEMON
	return EXT_VERSION_LAKEMON;
#else
	return "0.1.1";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(lakemon, loader) {
	duckdb::LoadInternal(loader);
}
}
