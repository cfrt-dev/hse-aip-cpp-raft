CXX ?= clang
CXXFLAGS ?= -std=c++17 -Wall -Wextra -Wpedantic -Iinclude -Oz
LDFLAGS ?=
LDLIBS ?= -pthread
GTEST_LIBS ?= -lgtest_main -lgtest

BUILD_DIR := build

RAFT_SRCS := \
    src/common.cpp \
    src/kv_store.cpp \
    src/logging.cpp \
    src/network.cpp \
    src/node.cpp \
    src/raft_core.cpp

APP_SRCS := src/main.cpp
TEST_SRCS := tests/test_raft_kv.cpp

RAFT_OBJS := $(RAFT_SRCS:src/%.cpp=$(BUILD_DIR)/src/%.o)
APP_OBJS := $(APP_SRCS:src/%.cpp=$(BUILD_DIR)/src/%.o)
TEST_OBJS := $(TEST_SRCS:tests/%.cpp=$(BUILD_DIR)/tests/%.o)

APP := $(BUILD_DIR)/raft_kv
TEST_APP := $(BUILD_DIR)/raft_kv_tests

.PHONY: all test clean

all: $(APP)

$(APP): $(RAFT_OBJS) $(APP_OBJS)
	$(CXX) $(LDFLAGS) -o $@ $^ $(LDLIBS)

$(TEST_APP): $(RAFT_OBJS) $(TEST_OBJS)
	$(CXX) $(LDFLAGS) -o $@ $^ $(LDLIBS) $(GTEST_LIBS)

$(BUILD_DIR)/src/%.o: src/%.cpp include/raft_kv.h src/raft_internal.h
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD_DIR)/tests/%.o: tests/%.cpp include/raft_kv.h
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -c $< -o $@

test: $(TEST_APP)
	$(TEST_APP)

clean:
	rm -rf $(BUILD_DIR)
