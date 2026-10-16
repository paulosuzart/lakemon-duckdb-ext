#pragma once

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

#include <new>

// Detect DuckDB 2.x by result_format.hpp, which exists only on 2.x.
// identifier.hpp is not a 2.x signal: it was backported to 1.5.6.
// Prefer the header probe over DUCKDB_MAJOR_VERSION: some CI toolchains
// omit that macro or set it unexpectedly.
#if defined(__has_include)
#if __has_include("duckdb/main/result_format.hpp")
#define LAKEMON_DUCKDB_2 1
#else
#define LAKEMON_DUCKDB_2 0
#endif
#elif defined(DUCKDB_MAJOR_VERSION) && DUCKDB_MAJOR_VERSION >= 2
#define LAKEMON_DUCKDB_2 1
#else
#define LAKEMON_DUCKDB_2 0
#endif

#if LAKEMON_DUCKDB_2
#include "duckdb/common/identifier.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/result_format.hpp"
#else
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#endif

#include <initializer_list>
#include <utility>

namespace lakemon {

#if LAKEMON_DUCKDB_2
typedef duckdb::QueryResult QueryHandle;
typedef duckdb::vector<duckdb::Identifier> ColumnNameList;
#else
typedef duckdb::MaterializedQueryResult QueryHandle;
typedef duckdb::vector<duckdb::string> ColumnNameList;
#endif

inline const char *SafeWhat(const std::exception &ex) {
	const char *msg = ex.what();
	return msg ? msg : "unknown error";
}

// Nested SQL on a fresh Connection so a failed DuckLake CALL does not poison
// the ClientContext that is executing lakemon_*. Interrupt and bad_alloc are rethrown.
inline duckdb::unique_ptr<QueryHandle> RunSQL(duckdb::ClientContext &context, const std::string &sql) {
	if (!context.db) {
		throw duckdb::InvalidInputException("lakemon: database handle is not available");
	}
	try {
		duckdb::Connection con(*context.db);
		auto result = con.Query(sql);
		if (!result) {
			throw duckdb::InvalidInputException("lakemon: query returned no result");
		}
		if (result->HasError()) {
			throw duckdb::InvalidInputException("lakemon: %s", result->GetError());
		}
		return result;
	} catch (const duckdb::InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const duckdb::Exception &) {
		throw;
	} catch (const std::exception &ex) {
		throw duckdb::InvalidInputException("lakemon: %s", SafeWhat(ex));
	}
}

// One cell from a materialized query. Out-of-range or expected DuckDB / benign
// read errors become a NULL Value. InterruptException and std::bad_alloc are
// rethrown so fatals are not turned into empty cells.
inline duckdb::Value CellAt(QueryHandle &result, duckdb::idx_t col, duckdb::idx_t row) {
	if (col >= result.ColumnCount()) {
		return duckdb::Value();
	}
	try {
#if LAKEMON_DUCKDB_2
		// QueryResult has no GetValue; Collection() Completes into a chunk CDC.
		return result.Collection<duckdb::ChunkFormat>().GetValue(col, row);
#else
		return result.GetValue(col, row);
#endif
	} catch (const duckdb::InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const duckdb::Exception &) {
		return duckdb::Value();
	} catch (const std::exception &) {
		return duckdb::Value();
	}
}

inline std::string CellString(QueryHandle &result, duckdb::idx_t col, duckdb::idx_t row) {
	const duckdb::Value value = CellAt(result, col, row);
	if (value.IsNull()) {
		return std::string();
	}
	return value.ToString();
}

inline double CellDouble(QueryHandle &result, duckdb::idx_t col, duckdb::idx_t row, double fallback = 0) {
	const duckdb::Value value = CellAt(result, col, row);
	if (value.IsNull()) {
		return fallback;
	}
	try {
		return value.GetValue<double>();
	} catch (const duckdb::InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const duckdb::Exception &) {
		return fallback;
	} catch (const std::exception &) {
		return fallback;
	}
}

// Row count of a successful DuckLake CALL result. 1.5 MaterializedQueryResult
// and 2.x QueryResult both expose RowCount() after materialize.
inline int64_t ResultRowCount(QueryHandle &result) {
	try {
		return static_cast<int64_t>(result.RowCount());
	} catch (const duckdb::InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const duckdb::Exception &) {
		return 0;
	} catch (const std::exception &) {
		return 0;
	}
}

inline int64_t CellInt64(QueryHandle &result, duckdb::idx_t col, duckdb::idx_t row, int64_t fallback = 0) {
	const duckdb::Value value = CellAt(result, col, row);
	if (value.IsNull()) {
		return fallback;
	}
	try {
		return value.GetValue<int64_t>();
	} catch (const duckdb::InterruptException &) {
		throw;
	} catch (const std::bad_alloc &) {
		throw;
	} catch (const duckdb::Exception &) {
		return fallback;
	} catch (const std::exception &) {
		return fallback;
	}
}

inline void ReferenceScalar(duckdb::Vector &result, const duckdb::Value &value) {
#if LAKEMON_DUCKDB_2
	result.Reference(value, duckdb::count_t(1));
#else
	result.Reference(value);
#endif
}

inline void WriteChunkValue(duckdb::DataChunk &output, duckdb::idx_t col, duckdb::idx_t row,
                            const duckdb::Value &value) {
#if LAKEMON_DUCKDB_2
	output.data[col].SetValue(row, value);
#else
	output.SetValue(col, row, value);
#endif
}

inline void FinishChunk(duckdb::DataChunk &output, duckdb::idx_t count) {
#if LAKEMON_DUCKDB_2
	output.SetChildCardinality(count);
#else
	output.SetCardinality(count);
#endif
}

// 1.5: named_parameters map. 2.x: typed **kwargs on the signature.
// Works for TableFunction and PragmaFunction.
template <typename FunctionT>
inline void AddNamedParameters(
    FunctionT &function,
    std::initializer_list<std::pair<const char *, duckdb::LogicalType>> params) {
#if LAKEMON_DUCKDB_2
	duckdb::TypedKwargs options;
	for (const auto &param : params) {
		options.Add(param.first, param.second);
	}
	function.GetSignature().AddTypedKwargs("options", std::move(options));
#else
	for (const auto &param : params) {
		function.named_parameters[param.first] = param.second;
	}
#endif
}

} // namespace lakemon
