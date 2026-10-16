# Included by DuckDB's build system. It specifies which extension to load.

duckdb_extension_load(lakemon
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)

# Postgres lake CI only: ducklake + postgres_scanner in the same unittest
# binary. Do not set LAKEMON_CI_POSTGRES on the distribution pipeline —
# ducklake needs roaring from vcpkg (not in this repo's manifest) and
# postgres_scanner needs libpq. linux_amd64 distribution still runs
# test/sql without ducklake (require ducklake skips execute tests).
# PRAGMA rewrite and missing ducklake CALLs must not throw INTERNAL.
if (DEFINED ENV{LAKEMON_CI_POSTGRES} AND NOT "$ENV{LAKEMON_CI_POSTGRES}" STREQUAL "")
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
      set(_lakemon_ducklake_patches "${CMAKE_CURRENT_LIST_DIR}/duckdb/.github/patches/extensions/ducklake")
      if (EXISTS "${_lakemon_ducklake_patches}")
        duckdb_extension_load(ducklake
          GIT_URL ${_lakemon_ducklake_url}
          GIT_TAG ${_lakemon_ducklake_tag}
          APPLY_PATCHES
        )
      else()
        duckdb_extension_load(ducklake
          GIT_URL ${_lakemon_ducklake_url}
          GIT_TAG ${_lakemon_ducklake_tag}
        )
      endif()
    endif()
  endif()

  if (NOT DEFINED WASM_ENABLED)
    set(WASM_ENABLED FALSE)
  endif()
  set(_lakemon_pg_cmake "${CMAKE_CURRENT_LIST_DIR}/duckdb/.github/config/extensions/postgres_scanner.cmake")
  if (EXISTS "${_lakemon_pg_cmake}")
    include("${_lakemon_pg_cmake}")
  endif()
endif()
