#ifndef RAFT_KV_H
#define RAFT_KV_H

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

#define RAFT_MAX_PEERS 16
#define RAFT_MAX_KEY 64
#define RAFT_MAX_VALUE 256
#define RAFT_MAX_LOG 4096
#define RAFT_MAX_LINE 1024
#define RAFT_MAX_PATH 256
#define RAFT_MAX_CONN 65536

typedef enum RaftRole {
    RAFT_FOLLOWER = 0,
    RAFT_CANDIDATE = 1,
    RAFT_LEADER = 2
} RaftRole;

typedef enum KvOp {
    KV_OP_NONE = 0,
    KV_OP_SET = 1,
    KV_OP_DEL = 2
} KvOp;

typedef struct KvItem {
    char key[RAFT_MAX_KEY];
    char value[RAFT_MAX_VALUE];
    uint8_t used;
} KvItem;

typedef struct KvStore {
    KvItem items[RAFT_MAX_LOG];
    uint32_t count;
} KvStore;

typedef struct RaftLogEntry {
    uint32_t index;
    uint32_t term;
    KvOp op;
    char key[RAFT_MAX_KEY];
    char value[RAFT_MAX_VALUE];
} RaftLogEntry;

typedef struct Peer {
    uint32_t id;
    char host[64];
    uint16_t port;
    int32_t next_index;
    int32_t match_index;
} Peer;

typedef struct ThreadJob {
    char line[RAFT_MAX_LINE];
    struct RaftNode *node;
    struct ThreadJob *next;
} ThreadJob;

typedef struct ThreadPool {
    pthread_t *threads;
    uint32_t thread_count;
    ThreadJob *head;
    ThreadJob *tail;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    uint8_t stop;
} ThreadPool;

typedef struct Connection {
    int32_t fd;
    char buffer[RAFT_MAX_LINE * 2];
    uint32_t length;
} Connection;

typedef struct RaftNode {
    uint32_t id;
    char host[64];
    uint16_t port;
    Peer peers[RAFT_MAX_PEERS];
    uint32_t peer_count;

    pthread_mutex_t mutex;
    RaftRole role;
    uint32_t current_term;
    int32_t voted_for;
    int32_t leader_id;
    uint32_t votes_received;

    RaftLogEntry log[RAFT_MAX_LOG];
    uint32_t log_count;
    uint32_t commit_index;
    uint32_t last_applied;
    KvStore store;

    int32_t server_fd;
    int32_t epoll_fd;
    Connection *connections[RAFT_MAX_CONN];
    ThreadPool pool;
    pthread_t network_thread;
    pthread_t ticker_thread;
    pthread_t stdin_thread;
    uint8_t running;
    uint64_t last_leader_contact_ms;
    uint64_t election_timeout_ms;

    char log_path[RAFT_MAX_PATH];
    FILE *event_log;
} RaftNode;

typedef struct RaftConfig {
    uint32_t id;
    char host[64];
    uint16_t port;
    Peer peers[RAFT_MAX_PEERS];
    uint32_t peer_count;
    char log_path[RAFT_MAX_PATH];
    uint32_t workers;
} RaftConfig;

/**
 * @brief Inserts, replaces or deletes a value in the in-memory key-value store.
 *
 * @param store Store to mutate. The pointer must not be NULL.
 * @param op Operation code: KV_OP_SET writes a value, KV_OP_DEL removes a key.
 * @param key Null-terminated key with length less than RAFT_MAX_KEY.
 * @param value Null-terminated value used only for KV_OP_SET.
 * @return 0 on success, negative value on invalid input or full store.
 */
int32_t kv_apply(KvStore *store, KvOp op, const char *key, const char *value);

/**
 * @brief Reads a value from the in-memory key-value store.
 *
 * @param store Store to inspect. The pointer must not be NULL.
 * @param key Null-terminated key with length less than RAFT_MAX_KEY.
 * @param out Buffer that receives the value.
 * @param out_size Size of out in bytes.
 * @return 0 when found, -1 when not found, -2 on invalid arguments.
 */
int32_t kv_get(const KvStore *store, const char *key, char *out, uint32_t out_size);

/**
 * @brief Parses a user command into a KV operation.
 *
 * @param line Input command such as "set a b", "del a" or "get a".
 * @param op Receives the parsed operation.
 * @param key Receives the parsed key.
 * @param value Receives the parsed value, if present.
 * @return 0 on success, negative value when the command is malformed or unknown.
 */
int32_t parse_kv_command(const char *line, KvOp *op, char *key, char *value);

int32_t raft_config_init(RaftConfig *config);
int32_t raft_config_parse(RaftConfig *config, int argc, char **argv);
void raft_config_print_usage(const char *program);

int32_t raft_node_init(RaftNode *node, const RaftConfig *config);
int32_t raft_node_start(RaftNode *node);
void raft_node_stop(RaftNode *node);
void raft_node_destroy(RaftNode *node);
void raft_handle_line(RaftNode *node, const char *line);
int32_t raft_submit_command(RaftNode *node, KvOp op, const char *key, const char *value);

#ifdef __cplusplus
}
#endif

#endif
