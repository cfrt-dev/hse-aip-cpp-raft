#define NOB_IMPLEMENTATION
#include "nob.h"

#include <stdlib.h>
#include <string.h>

typedef struct {
    const char *src;
    const char *obj;
    const char **deps;
    size_t deps_count;
} Build_Unit;

static const char *build_dir = "build";
static const char *app_path = "build/raft_kv";
static const char *test_path = "build/raft_kv_tests";

static const char *raft_deps[] = {
    "include/raft_kv.hpp",
    "src/raft_internal.hpp",
};

static const char *test_deps[] = {
    "include/raft_kv.hpp",
};

static const Build_Unit raft_units[] = {
    {"src/common.cpp", "build/src/common.o", raft_deps, NOB_ARRAY_LEN(raft_deps)},
    {"src/kv_store.cpp", "build/src/kv_store.o", raft_deps, NOB_ARRAY_LEN(raft_deps)},
    {"src/logging.cpp", "build/src/logging.o", raft_deps, NOB_ARRAY_LEN(raft_deps)},
    {"src/network.cpp", "build/src/network.o", raft_deps, NOB_ARRAY_LEN(raft_deps)},
    {"src/node.cpp", "build/src/node.o", raft_deps, NOB_ARRAY_LEN(raft_deps)},
    {"src/raft_core.cpp", "build/src/raft_core.o", raft_deps, NOB_ARRAY_LEN(raft_deps)},
};

static const Build_Unit app_units[] = {
    {"src/main.cpp", "build/src/main.o", raft_deps, NOB_ARRAY_LEN(raft_deps)},
};

static const Build_Unit test_units[] = {
    {"tests/test_raft_kv.cpp", "build/tests/test_raft_kv.o", test_deps, NOB_ARRAY_LEN(test_deps)},
};

static void append_words(Nob_Cmd *cmd, const char *words) {
    if (words == NULL)
        return;

    while (*words != '\0') {
        while (*words == ' ' || *words == '\t' || *words == '\n')
            words++;
        if (*words == '\0')
            break;

        const char *begin = words;
        while (*words != '\0' && *words != ' ' && *words != '\t' && *words != '\n')
            words++;
        nob_cmd_append(cmd, nob_temp_sprintf("%.*s", (int)(words - begin), begin));
    }
}

static const char *env_or_default(const char *name, const char *default_value) {
    const char *value = getenv(name);
    return value != NULL && value[0] != '\0' ? value : default_value;
}

static bool ensure_build_dirs(void) {
    return nob_mkdir_if_not_exists(build_dir) &&
           nob_mkdir_if_not_exists("build/src") &&
           nob_mkdir_if_not_exists("build/tests");
}

static bool build_object(const Build_Unit *unit) {
    const char *inputs[8] = {0};
    size_t inputs_count = 0;
    inputs[inputs_count++] = unit->src;
    for (size_t i = 0; i < unit->deps_count; ++i) {
        inputs[inputs_count++] = unit->deps[i];
    }

    int rebuild = nob_needs_rebuild(unit->obj, inputs, inputs_count);
    if (rebuild < 0)
        return false;
    if (!rebuild)
        return true;

    Nob_Cmd cmd = {0};
    nob_cmd_append(&cmd, env_or_default("CXX", "clang++"));
    append_words(&cmd, env_or_default("CXXFLAGS", "-std=c++20 -Wall -Wextra -Wpedantic -Iinclude -Oz"));
    nob_cmd_append(&cmd, "-c", unit->src, "-o", unit->obj);
    return nob_cmd_run(&cmd);
}

static bool build_objects(const Build_Unit *units, size_t units_count) {
    for (size_t i = 0; i < units_count; ++i) {
        if (!build_object(&units[i]))
            return false;
    }
    return true;
}

static bool link_binary(const char *output, const char **objects, size_t objects_count, bool with_gtest) {
    int rebuild = nob_needs_rebuild(output, objects, objects_count);
    if (rebuild < 0)
        return false;
    if (!rebuild)
        return true;

    Nob_Cmd cmd = {0};
    nob_cmd_append(&cmd, env_or_default("CXX", "clang++"));
    append_words(&cmd, getenv("LDFLAGS"));
    nob_cmd_append(&cmd, "-o", output);
    for (size_t i = 0; i < objects_count; ++i) {
        nob_cmd_append(&cmd, objects[i]);
    }
    append_words(&cmd, env_or_default("LDLIBS", "-pthread"));
    if (with_gtest) {
        append_words(&cmd, env_or_default("GTEST_LIBS", "-lgtest_main -lgtest"));
    }
    return nob_cmd_run(&cmd);
}

static bool build_app(void) {
    if (!ensure_build_dirs())
        return false;
    if (!build_objects(raft_units, NOB_ARRAY_LEN(raft_units)))
        return false;
    if (!build_objects(app_units, NOB_ARRAY_LEN(app_units)))
        return false;

    const char *objects[] = {
        "build/src/common.o",
        "build/src/kv_store.o",
        "build/src/logging.o",
        "build/src/network.o",
        "build/src/node.o",
        "build/src/raft_core.o",
        "build/src/main.o",
    };
    return link_binary(app_path, objects, NOB_ARRAY_LEN(objects), false);
}

static bool build_tests(void) {
    if (!ensure_build_dirs())
        return false;
    if (!build_objects(raft_units, NOB_ARRAY_LEN(raft_units)))
        return false;
    if (!build_objects(test_units, NOB_ARRAY_LEN(test_units)))
        return false;

    const char *objects[] = {
        "build/src/common.o",
        "build/src/kv_store.o",
        "build/src/logging.o",
        "build/src/network.o",
        "build/src/node.o",
        "build/src/raft_core.o",
        "build/tests/test_raft_kv.o",
    };
    return link_binary(test_path, objects, NOB_ARRAY_LEN(objects), true);
}

static bool run_tests(void) {
    if (!build_tests())
        return false;

    Nob_Cmd cmd = {0};
    nob_cmd_append(&cmd, test_path);
    return nob_cmd_run(&cmd);
}

static bool delete_entry(Nob_Walk_Entry entry) {
    (void)entry.data;
    (void)entry.level;
    (void)entry.type;
    (void)entry.action;
    return nob_delete_file(entry.path);
}

static bool clean(void) {
    if (!nob_file_exists(build_dir))
        return true;
    return nob_walk_dir(build_dir, delete_entry, .post_order = true);
}

static void usage(const char *program) {
    nob_log(NOB_INFO, "usage: %s [all|test|clean|help]", program);
}

int main(int argc, char **argv) {
    NOB_GO_REBUILD_URSELF(argc, argv);

    const char *program = nob_shift(argv, argc);
    const char *command = argc > 0 ? nob_shift(argv, argc) : "all";

    if (strcmp(command, "all") == 0) {
        return build_app() ? 0 : 1;
    }
    if (strcmp(command, "test") == 0) {
        return run_tests() ? 0 : 1;
    }
    if (strcmp(command, "clean") == 0) {
        return clean() ? 0 : 1;
    }
    if (strcmp(command, "help") == 0 || strcmp(command, "-h") == 0 || strcmp(command, "--help") == 0) {
        usage(program);
        return 0;
    }

    nob_log(NOB_ERROR, "unknown command: %s", command);
    usage(program);
    return 1;
}
