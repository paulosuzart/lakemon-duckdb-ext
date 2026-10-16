PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

CXX ?= g++

EXT_NAME=lakemon
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

-include extension-ci-tools/makefiles/duckdb_extension.Makefile

.PHONY: policy-test
policy-test:
	$(CXX) -std=c++17 -O2 -Wall -Wextra -I$(PROJ_DIR)src/include -I$(PROJ_DIR)test/policy \
		-o $(PROJ_DIR)test/policy/test_policy $(PROJ_DIR)test/policy/test_policy.cpp
	$(PROJ_DIR)test/policy/test_policy
	$(CXX) -std=c++17 -O2 -Wall -Wextra -I$(PROJ_DIR)src/include -I$(PROJ_DIR)test/policy \
		-o $(PROJ_DIR)test/policy/test_options $(PROJ_DIR)test/policy/test_options.cpp
	$(PROJ_DIR)test/policy/test_options
	$(CXX) -std=c++17 -O2 -Wall -Wextra -I$(PROJ_DIR)src/include -I$(PROJ_DIR)test/policy \
		-o $(PROJ_DIR)test/policy/test_overrides $(PROJ_DIR)test/policy/test_overrides.cpp
	$(PROJ_DIR)test/policy/test_overrides
	$(CXX) -std=c++17 -O2 -Wall -Wextra -I$(PROJ_DIR)src/include -I$(PROJ_DIR)test/policy \
		-o $(PROJ_DIR)test/policy/test_sql $(PROJ_DIR)test/policy/test_sql.cpp
	$(PROJ_DIR)test/policy/test_sql

# Execute flush+merge and expire against a real lake. A hang fails via timeout.
.PHONY: lake-test
LAKE_TEST_TIMEOUT ?= 60
lake-test:
	python3 $(PROJ_DIR)test/run_execute.py $(LAKE_TEST_TIMEOUT)
