#ifndef RAFT_INTERNAL_H
#define RAFT_INTERNAL_H

#include "raft_kv.h"

#include <stdint.h>

uint64_t now_ms(void);
int32_t set_nonblock(int32_t fd);
void copy_text(char *dst, uint32_t dst_size, const char *src);
int32_t is_valid_token(const char *text, uint32_t max_size);
const char *role_name(RaftRole role);
const char *op_name(KvOp op);
KvOp op_from_name(const char *name);

void log_event(RaftNode *node, const char *format, ...);
void open_event_log(RaftNode *node);

Peer *find_peer(RaftNode *node, uint32_t id);
int32_t add_peer_locked(RaftNode *node, uint32_t id, const char *host, uint16_t port);
int32_t remove_peer_locked(RaftNode *node, uint32_t id);
int32_t majority_count(const RaftNode *node);
uint32_t last_log_index_locked(const RaftNode *node);
uint32_t last_log_term_locked(const RaftNode *node);
uint64_t random_election_timeout(const RaftNode *node);

void apply_committed_locked(RaftNode *node);
void become_follower_locked(RaftNode *node, uint32_t term, int32_t leader_id);
void start_election(RaftNode *node);
void request_cluster_join(RaftNode *node);
void request_cluster_leave(RaftNode *node);
void broadcast_append(RaftNode *node);
void send_append_to_peer(RaftNode *node, uint32_t peer_id);
int32_t send_to_peer(RaftNode *node, uint32_t peer_id, const char *line);
int32_t connect_send(const char *host, uint16_t port, const char *line);

int32_t pool_init(ThreadPool *pool, uint32_t thread_count);
int32_t pool_submit(ThreadPool *pool, RaftNode *node, const char *line);
void pool_destroy(ThreadPool *pool);

int32_t create_server(RaftNode *node);
void close_connection(RaftNode *node, int32_t fd);
void *network_loop(void *arg);
void *ticker_loop(void *arg);
void *stdin_loop(void *arg);

#endif
