# Included by DuckDB's build system. It specifies which extension to load.

duckdb_extension_load(lakemon
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)

# File-backed lake execute tests need ducklake in the same unittest binary.
# Pin GIT_URL / GIT_TAG from DuckDB's own ducklake.cmake (version-matched).
# Skip LOAD_TESTS. APPLY_PATCHES uses patches from the DuckDB source tree.
set(_lakemon_ducklake_cmake "${CMAKE_CURRENT_LIST_DIR}/duckdb/.github/config/extensions/ducklake.cmake")
if (EXISTS "${_lakemon_ducklake_cmake}")
  file(READ "${_lakemon_ducklake_cmake}" _lakemon_ducklake_contents)
  string(REGEX MATCH "GIT_URL[ \t]+([^ \t\n)]+)" _lakemon_ducklake_url_match "${_lakemon_ducklake_contents}")
  set(_lakemon_ducklake_url "${CMAKE_MATCH_1}")
  string(REGEX MATCH "GIT_TAG[ \t]+([0-9a-fA-F]+)" _lakemon_ducklake_tag_match "${_lakemon_ducklake_contents}")
  set(_lakemon_ducklake_tag "${CMAKE_MATCH_1}")
  if (_lakemon_ducklake_url AND _lakemon_ducklake_tag AND NOT EMSCRIPTEN)
    duckdb_extension_load(ducklake
      GIT_URL ${_lakemon_ducklake_url}
      GIT_TAG ${_lakemon_ducklake_tag}
      APPLY_PATCHES
    )
  endif()
endif()

