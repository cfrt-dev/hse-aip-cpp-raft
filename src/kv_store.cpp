#include "raft_internal.h"

#include <stdio.h>
#include <string.h>

int32_t kv_apply(KvStore *store, KvOp op, const char *key, const char *value) {
    uint32_t i;
    int32_t free_index = -1;

    if (store == NULL || !is_valid_token(key, RAFT_MAX_KEY)) {
        return -2;
    }
    if (op == KV_OP_SET && !is_valid_token(value, RAFT_MAX_VALUE)) {
        return -2;
    }
    if (op != KV_OP_SET && op != KV_OP_DEL) {
        return -3;
    }

    for (i = 0; i < RAFT_MAX_LOG; ++i) {
        if (store->items[i].used != 0 && strcmp(store->items[i].key, key) == 0) {
            if (op == KV_OP_DEL) {
                memset(&store->items[i], 0, sizeof(store->items[i]));
                if (store->count > 0) {
                    store->count--;
                }
                return 0;
            }
            copy_text(store->items[i].value, RAFT_MAX_VALUE, value);
            return 0;
        }
        if (free_index < 0 && store->items[i].used == 0) {
            free_index = (int32_t)i;
        }
    }

    if (op == KV_OP_DEL) {
        return 0;
    }
    if (free_index < 0) {
        return -4;
    }

    store->items[free_index].used = 1;
    copy_text(store->items[free_index].key, RAFT_MAX_KEY, key);
    copy_text(store->items[free_index].value, RAFT_MAX_VALUE, value);
    store->count++;
    return 0;
}

int32_t kv_get(const KvStore *store, const char *key, char *out, uint32_t out_size) {
    uint32_t i;
    if (store == NULL || !is_valid_token(key, RAFT_MAX_KEY) || out == NULL || out_size == 0) {
        return -2;
    }
    for (i = 0; i < RAFT_MAX_LOG; ++i) {
        if (store->items[i].used != 0 && strcmp(store->items[i].key, key) == 0) {
            copy_text(out, out_size, store->items[i].value);
            return 0;
        }
    }
    return -1;
}

int32_t parse_kv_command(const char *line, KvOp *op, char *key, char *value) {
    char command[16];
    char local_key[RAFT_MAX_KEY];
    char local_value[RAFT_MAX_VALUE];
    int32_t fields;

    if (line == NULL || op == NULL || key == NULL || value == NULL) {
        return -1;
    }

    memset(command, 0, sizeof(command));
    memset(local_key, 0, sizeof(local_key));
    memset(local_value, 0, sizeof(local_value));
    fields = sscanf(line, "%15s %63s %255s", command, local_key, local_value);

    if (fields >= 1 && strcmp(command, "set") == 0) {
        if (fields != 3 || !is_valid_token(local_key, RAFT_MAX_KEY) || !is_valid_token(local_value, RAFT_MAX_VALUE)) {
            return -2;
        }
        *op = KV_OP_SET;
        copy_text(key, RAFT_MAX_KEY, local_key);
        copy_text(value, RAFT_MAX_VALUE, local_value);
        return 0;
    }
    if (fields >= 1 && strcmp(command, "del") == 0) {
        if (fields != 2 || !is_valid_token(local_key, RAFT_MAX_KEY)) {
            return -2;
        }
        *op = KV_OP_DEL;
        copy_text(key, RAFT_MAX_KEY, local_key);
        value[0] = '\0';
        return 0;
    }
    if (fields >= 1 && strcmp(command, "get") == 0) {
        if (fields != 2 || !is_valid_token(local_key, RAFT_MAX_KEY)) {
            return -2;
        }
        *op = KV_OP_NONE;
        copy_text(key, RAFT_MAX_KEY, local_key);
        value[0] = '\0';
        return 0;
    }
    return -3;
}
