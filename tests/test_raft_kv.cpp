#include "raft_kv.h"

#include <gtest/gtest.h>

#include <string.h>

TEST(KvStore, ApplySetReplaceDeleteAndGet) {
    KvStore store;
    char out[RAFT_MAX_VALUE];
    memset(&store, 0, sizeof(store));
    memset(out, 0, sizeof(out));

    EXPECT_EQ(kv_apply(&store, KV_OP_SET, "a", "1"), 0);
    EXPECT_EQ(kv_get(&store, "a", out, sizeof(out)), 0);
    EXPECT_STREQ(out, "1");

    EXPECT_EQ(kv_apply(&store, KV_OP_SET, "a", "2"), 0);
    EXPECT_EQ(kv_get(&store, "a", out, sizeof(out)), 0);
    EXPECT_STREQ(out, "2");

    EXPECT_EQ(kv_apply(&store, KV_OP_DEL, "a", ""), 0);
    EXPECT_EQ(kv_get(&store, "a", out, sizeof(out)), -1);
}

TEST(KvStore, RejectsInvalidInput) {
    KvStore store;
    char out[2];
    memset(&store, 0, sizeof(store));

    EXPECT_LT(kv_apply(NULL, KV_OP_SET, "a", "1"), 0);
    EXPECT_LT(kv_apply(&store, KV_OP_SET, "", "1"), 0);
    EXPECT_LT(kv_apply(&store, KV_OP_SET, "has space", "1"), 0);
    EXPECT_LT(kv_get(NULL, "a", out, sizeof(out)), 0);
    EXPECT_LT(kv_get(&store, "a", NULL, sizeof(out)), 0);
}

TEST(Parser, ParsesKnownCommands) {
    KvOp op;
    char key[RAFT_MAX_KEY];
    char value[RAFT_MAX_VALUE];

    memset(key, 0, sizeof(key));
    memset(value, 0, sizeof(value));
    EXPECT_EQ(parse_kv_command("set alpha beta", &op, key, value), 0);
    EXPECT_EQ(op, KV_OP_SET);
    EXPECT_STREQ(key, "alpha");
    EXPECT_STREQ(value, "beta");

    EXPECT_EQ(parse_kv_command("del alpha", &op, key, value), 0);
    EXPECT_EQ(op, KV_OP_DEL);
    EXPECT_STREQ(key, "alpha");

    EXPECT_EQ(parse_kv_command("get alpha", &op, key, value), 0);
    EXPECT_EQ(op, KV_OP_NONE);
    EXPECT_STREQ(key, "alpha");
}

TEST(Parser, RejectsMalformedCommands) {
    KvOp op;
    char key[RAFT_MAX_KEY];
    char value[RAFT_MAX_VALUE];

    EXPECT_LT(parse_kv_command("unknown alpha", &op, key, value), 0);
    EXPECT_LT(parse_kv_command("set onlykey", &op, key, value), 0);
    EXPECT_LT(parse_kv_command("del", &op, key, value), 0);
    EXPECT_LT(parse_kv_command(NULL, &op, key, value), 0);
}
