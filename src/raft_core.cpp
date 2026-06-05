#include "raft_internal.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>

void become_follower_locked(RaftNode *node, uint32_t term, int32_t leader_id) {
    if (term > node->current_term) {
        node->current_term = term;
        node->voted_for = -1;
    }
    if (node->role != RAFT_FOLLOWER) {
        log_event(node, "becomes follower for term %u", node->current_term);
    }
    node->role = RAFT_FOLLOWER;
    node->leader_id = leader_id;
    node->votes_received = 0;
    node->last_leader_contact_ms = now_ms();
    node->election_timeout_ms = random_election_timeout(node);
}

void apply_committed_locked(RaftNode *node) {
    while (node->last_applied < node->commit_index) {
        RaftLogEntry *entry = &node->log[node->last_applied];
        if (entry->op == KV_OP_SET || entry->op == KV_OP_DEL) {
            kv_apply(&node->store, entry->op, entry->key, entry->value);
            log_event(node, "applied log[%u] %s %s %s", entry->index, op_name(entry->op), entry->key, entry->value);
        }
        node->last_applied++;
    }
}

static int32_t append_entry_locked(RaftNode *node, KvOp op, const char *key, const char *value) {
    RaftLogEntry *entry;
    if (node->log_count >= RAFT_MAX_LOG) {
        return -1;
    }
    entry = &node->log[node->log_count];
    memset(entry, 0, sizeof(*entry));
    entry->index = node->log_count + 1u;
    entry->term = node->current_term;
    entry->op = op;
    copy_text(entry->key, RAFT_MAX_KEY, key);
    copy_text(entry->value, RAFT_MAX_VALUE, value);
    node->log_count++;
    return (int32_t)entry->index;
}

static void recompute_commit_locked(RaftNode *node) {
    int32_t majority = majority_count(node);
    int32_t count;
    uint32_t index;
    uint32_t i;

    if (node->role != RAFT_LEADER) {
        return;
    }
    for (index = node->log_count; index > node->commit_index; --index) {
        if (node->log[index - 1u].term != node->current_term) {
            continue;
        }
        count = 1;
        for (i = 0; i < node->peer_count; ++i) {
            if (node->peers[i].match_index >= (int32_t)index) {
                count++;
            }
        }
        if (count >= majority) {
            node->commit_index = index;
            log_event(node, "committed through index %u", node->commit_index);
            apply_committed_locked(node);
            return;
        }
    }
}

static void become_leader_locked(RaftNode *node) {
    uint32_t i;
    node->role = RAFT_LEADER;
    node->leader_id = (int32_t)node->id;
    for (i = 0; i < node->peer_count; ++i) {
        node->peers[i].next_index = (int32_t)node->log_count + 1;
        node->peers[i].match_index = 0;
    }
    log_event(node, "becomes leader for term %u", node->current_term);
}

void start_election(RaftNode *node) {
    char line[RAFT_MAX_LINE];
    Peer peers[RAFT_MAX_PEERS];
    uint32_t peer_count;
    uint32_t term;
    uint32_t last_index;
    uint32_t last_term;
    uint32_t i;
    int32_t became_leader = 0;

    pthread_mutex_lock(&node->mutex);
    peer_count = node->peer_count;
    for (i = 0; i < peer_count; ++i) {
        peers[i] = node->peers[i];
    }
    node->role = RAFT_CANDIDATE;
    node->current_term++;
    node->voted_for = (int32_t)node->id;
    node->votes_received = 1;
    node->leader_id = -1;
    node->last_leader_contact_ms = now_ms();
    node->election_timeout_ms = random_election_timeout(node);
    term = node->current_term;
    last_index = last_log_index_locked(node);
    last_term = last_log_term_locked(node);
    if ((int32_t)node->votes_received >= majority_count(node)) {
        become_leader_locked(node);
        became_leader = 1;
    }
    pthread_mutex_unlock(&node->mutex);

    log_event(node, "starts election for term %u", term);
    if (became_leader != 0) {
        broadcast_append(node);
        return;
    }
    snprintf(line, sizeof(line), "RV %u %u %u %u\n", term, node->id, last_index, last_term);
    for (i = 0; i < peer_count; ++i) {
        connect_send(peers[i].host, peers[i].port, line);
    }
}

static void announce_peer_to_known_nodes(RaftNode *node, uint32_t peer_id, const char *host, uint16_t port,
                                         uint32_t except_id) {
    Peer peers[RAFT_MAX_PEERS];
    uint32_t count;
    uint32_t i;
    char line[RAFT_MAX_LINE];

    count = 0;
    pthread_mutex_lock(&node->mutex);
    for (i = 0; i < node->peer_count; ++i) {
        peers[count++] = node->peers[i];
    }
    pthread_mutex_unlock(&node->mutex);

    snprintf(line, sizeof(line), "PEER %u %s %u\n", peer_id, host, (uint32_t)port);
    for (i = 0; i < count; ++i) {
        if (peers[i].id != except_id && peers[i].id != peer_id) {
            connect_send(peers[i].host, peers[i].port, line);
        }
    }
}

static void send_peer_catalog(RaftNode *node, const char *host, uint16_t port) {
    Peer peers[RAFT_MAX_PEERS];
    uint32_t count;
    uint32_t i;
    char line[RAFT_MAX_LINE];

    count = 0;
    pthread_mutex_lock(&node->mutex);
    for (i = 0; i < node->peer_count; ++i) {
        peers[count++] = node->peers[i];
    }
    pthread_mutex_unlock(&node->mutex);

    snprintf(line, sizeof(line), "PEER %u %s %u\n", node->id, node->host, (uint32_t)node->port);
    connect_send(host, port, line);

    for (i = 0; i < count; ++i) {
        snprintf(line, sizeof(line), "PEER %u %s %u\n", peers[i].id, peers[i].host, (uint32_t)peers[i].port);
        connect_send(host, port, line);
    }
}

void request_cluster_join(RaftNode *node) {
    Peer peers[RAFT_MAX_PEERS];
    uint32_t count;
    uint32_t i;
    char line[RAFT_MAX_LINE];

    count = 0;
    pthread_mutex_lock(&node->mutex);
    for (i = 0; i < node->peer_count; ++i) {
        peers[count++] = node->peers[i];
    }
    pthread_mutex_unlock(&node->mutex);

    snprintf(line, sizeof(line), "JOIN %u %s %u\n", node->id, node->host, (uint32_t)node->port);
    for (i = 0; i < count; ++i) {
        connect_send(peers[i].host, peers[i].port, line);
    }
}

static void announce_leave_to_known_nodes(RaftNode *node, uint32_t peer_id) {
    Peer peers[RAFT_MAX_PEERS];
    uint32_t count;
    uint32_t i;
    char line[RAFT_MAX_LINE];

    count = 0;
    pthread_mutex_lock(&node->mutex);
    for (i = 0; i < node->peer_count; ++i) {
        peers[count++] = node->peers[i];
    }
    pthread_mutex_unlock(&node->mutex);

    snprintf(line, sizeof(line), "LEAVE %u\n", peer_id);
    for (i = 0; i < count; ++i) {
        if (peers[i].id != peer_id) {
            connect_send(peers[i].host, peers[i].port, line);
        }
    }
}

void request_cluster_leave(RaftNode *node) {
    Peer peers[RAFT_MAX_PEERS];
    uint32_t count;
    uint32_t i;
    char line[RAFT_MAX_LINE];

    if (node == NULL) {
        return;
    }

    count = 0;
    pthread_mutex_lock(&node->mutex);
    for (i = 0; i < node->peer_count; ++i) {
        peers[count++] = node->peers[i];
    }
    pthread_mutex_unlock(&node->mutex);

    snprintf(line, sizeof(line), "LEAVE %u\n", node->id);
    for (i = 0; i < count; ++i) {
        connect_send(peers[i].host, peers[i].port, line);
    }
    log_event(node, "announced graceful leave to %u peer(s)", count);
}

static void handle_join(RaftNode *node, uint32_t peer_id, const char *host, uint16_t port) {
    int32_t add_result;

    pthread_mutex_lock(&node->mutex);
    add_result = add_peer_locked(node, peer_id, host, port);
    pthread_mutex_unlock(&node->mutex);

    if (add_result > 0) {
        log_event(node, "discovered peer %u at %s:%u from join", peer_id, host, (uint32_t)port);
        announce_peer_to_known_nodes(node, peer_id, host, port, peer_id);
    }
    send_peer_catalog(node, host, port);
}

static void handle_peer_announce(RaftNode *node, uint32_t peer_id, const char *host, uint16_t port) {
    int32_t add_result;

    pthread_mutex_lock(&node->mutex);
    add_result = add_peer_locked(node, peer_id, host, port);
    pthread_mutex_unlock(&node->mutex);

    if (add_result > 0) {
        log_event(node, "discovered peer %u at %s:%u", peer_id, host, (uint32_t)port);
        announce_peer_to_known_nodes(node, peer_id, host, port, peer_id);
        send_append_to_peer(node, peer_id);
    }
}

static void handle_leave(RaftNode *node, uint32_t peer_id) {
    int32_t remove_result;

    pthread_mutex_lock(&node->mutex);
    remove_result = remove_peer_locked(node, peer_id);
    pthread_mutex_unlock(&node->mutex);

    if (remove_result > 0) {
        log_event(node, "removed peer %u after graceful leave", peer_id);
        announce_leave_to_known_nodes(node, peer_id);
    }
}

static void handle_request_vote(RaftNode *node, uint32_t term, uint32_t candidate_id, uint32_t last_index,
                                uint32_t last_term) {
    char line[RAFT_MAX_LINE];
    int32_t grant = 0;
    uint32_t local_last_index;
    uint32_t local_last_term;
    uint32_t current_term;

    pthread_mutex_lock(&node->mutex);
    if (term > node->current_term) {
        become_follower_locked(node, term, -1);
    }
    local_last_index = last_log_index_locked(node);
    local_last_term = last_log_term_locked(node);
    if (term == node->current_term && (node->voted_for < 0 || node->voted_for == (int32_t)candidate_id)) {
        if (last_term > local_last_term || (last_term == local_last_term && last_index >= local_last_index)) {
            grant = 1;
            node->voted_for = (int32_t)candidate_id;
            node->last_leader_contact_ms = now_ms();
        }
    }
    current_term = node->current_term;
    pthread_mutex_unlock(&node->mutex);

    snprintf(line, sizeof(line), "RVR %u %u %d\n", current_term, node->id, grant);
    send_to_peer(node, candidate_id, line);
    log_event(node, "vote request from %u term %u result %d", candidate_id, term, grant);
}

static void handle_request_vote_response(RaftNode *node, uint32_t term, uint32_t voter_id, uint32_t granted) {
    int32_t became_leader = 0;

    pthread_mutex_lock(&node->mutex);
    if (term > node->current_term) {
        become_follower_locked(node, term, -1);
        pthread_mutex_unlock(&node->mutex);
        return;
    }
    if (node->role == RAFT_CANDIDATE && term == node->current_term && granted != 0) {
        node->votes_received++;
        log_event(node, "received vote from %u (%u/%d)", voter_id, node->votes_received, majority_count(node));
        if ((int32_t)node->votes_received >= majority_count(node)) {
            become_leader_locked(node);
            became_leader = 1;
        }
    }
    pthread_mutex_unlock(&node->mutex);

    if (became_leader != 0) {
        broadcast_append(node);
    }
}

static void handle_append_entries(RaftNode *node, uint32_t term, uint32_t leader_id, uint32_t prev_index,
                                  uint32_t prev_term, uint32_t leader_commit, uint32_t has_entry,
                                  const RaftLogEntry *incoming) {
    char line[RAFT_MAX_LINE];
    int32_t success = 0;
    uint32_t match_index = 0;
    uint32_t current_term;

    pthread_mutex_lock(&node->mutex);
    if (term >= node->current_term) {
        become_follower_locked(node, term, (int32_t)leader_id);
    }

    if (term == node->current_term) {
        if (prev_index == 0 || (prev_index <= node->log_count && node->log[prev_index - 1u].term == prev_term)) {
            success = 1;
            if (has_entry != 0) {
                if (incoming->index <= node->log_count) {
                    if (node->log[incoming->index - 1u].term != incoming->term) {
                        node->log_count = incoming->index - 1u;
                    }
                }
                if (incoming->index == node->log_count + 1u && node->log_count < RAFT_MAX_LOG) {
                    node->log[node->log_count] = *incoming;
                    node->log_count++;
                    log_event(node, "received log[%u] %s %s %s", incoming->index, op_name(incoming->op), incoming->key,
                              incoming->value);
                }
            }
            if (leader_commit > node->commit_index) {
                node->commit_index = leader_commit < node->log_count ? leader_commit : node->log_count;
                apply_committed_locked(node);
            }
            match_index = node->log_count;
        }
    }
    current_term = node->current_term;
    pthread_mutex_unlock(&node->mutex);

    snprintf(line, sizeof(line), "AER %u %u %d %u\n", current_term, node->id, success, match_index);
    send_to_peer(node, leader_id, line);
}

static void handle_append_response(RaftNode *node, uint32_t term, uint32_t peer_id, uint32_t success,
                                   uint32_t match_index) {
    int32_t retry = 0;

    pthread_mutex_lock(&node->mutex);
    if (term > node->current_term) {
        become_follower_locked(node, term, -1);
        pthread_mutex_unlock(&node->mutex);
        return;
    }
    if (node->role == RAFT_LEADER && term == node->current_term) {
        Peer *peer = find_peer(node, peer_id);
        if (peer != NULL) {
            if (success != 0) {
                peer->match_index = (int32_t)match_index;
                peer->next_index = (int32_t)match_index + 1;
                recompute_commit_locked(node);
            } else {
                if (peer->next_index > 1) {
                    peer->next_index--;
                }
                retry = 1;
            }
        }
    }
    pthread_mutex_unlock(&node->mutex);

    if (retry != 0) {
        send_append_to_peer(node, peer_id);
    }
}

int32_t raft_submit_command(RaftNode *node, KvOp op, const char *key, const char *value) {
    char line[RAFT_MAX_LINE];
    int32_t leader_id;
    uint32_t index;

    if (node == NULL || (op != KV_OP_SET && op != KV_OP_DEL) || !is_valid_token(key, RAFT_MAX_KEY)) {
        return -1;
    }
    if (op == KV_OP_SET && !is_valid_token(value, RAFT_MAX_VALUE)) {
        return -1;
    }

    pthread_mutex_lock(&node->mutex);
    if (node->role == RAFT_LEADER) {
        index = (uint32_t)append_entry_locked(node, op, key, value);
        log_event(node, "accepted client command as log[%u] %s %s %s", index, op_name(op), key,
                  value == NULL ? "" : value);
        pthread_mutex_unlock(&node->mutex);
        broadcast_append(node);
        if (node->peer_count == 0) {
            pthread_mutex_lock(&node->mutex);
            node->commit_index = node->log_count;
            apply_committed_locked(node);
            pthread_mutex_unlock(&node->mutex);
        }
        return 0;
    }
    leader_id = node->leader_id;
    pthread_mutex_unlock(&node->mutex);

    if (leader_id >= 0) {
        snprintf(line, sizeof(line), "CL %u %s %s %s\n", node->id, op_name(op), key,
                 value == NULL || value[0] == '\0' ? "-" : value);
        if (send_to_peer(node, (uint32_t)leader_id, line) == 0) {
            log_event(node, "forwarded client command to leader %d", leader_id);
            return 0;
        }
    }

    log_event(node, "cannot accept command: no reachable leader");
    return -2;
}

static void handle_client_forward(RaftNode *node, uint32_t source_id, KvOp op, const char *key, const char *value) {
    int32_t rc = raft_submit_command(node, op, key, value);
    log_event(node, "client forward from %u result %d", source_id, rc);
}

void raft_handle_line(RaftNode *node, const char *line) {
    char type[8];
    uint32_t term;
    uint32_t id;
    uint32_t a;
    uint32_t b;
    uint32_t c;
    uint32_t d;
    char host[64];
    char op_text[16];
    char key[RAFT_MAX_KEY];
    char value[RAFT_MAX_VALUE];
    RaftLogEntry entry;

    if (node == NULL || line == NULL) {
        return;
    }
    memset(type, 0, sizeof(type));
    if (sscanf(line, "%7s", type) != 1) {
        return;
    }

    if (strcmp(type, "RV") == 0) {
        if (sscanf(line, "RV %u %u %u %u", &term, &id, &a, &b) == 4) {
            handle_request_vote(node, term, id, a, b);
        }
        return;
    }
    if (strcmp(type, "RVR") == 0) {
        if (sscanf(line, "RVR %u %u %u", &term, &id, &a) == 3) {
            handle_request_vote_response(node, term, id, a);
        }
        return;
    }
    if (strcmp(type, "AE") == 0) {
        memset(&entry, 0, sizeof(entry));
        memset(op_text, 0, sizeof(op_text));
        memset(key, 0, sizeof(key));
        memset(value, 0, sizeof(value));
        if (sscanf(line, "AE %u %u %u %u %u %u %u %u %15s %63s %255s", &term, &id, &a, &b, &c, &d, &entry.index,
                   &entry.term, op_text, key, value) == 11) {
            entry.op = op_from_name(op_text);
            copy_text(entry.key, RAFT_MAX_KEY, strcmp(key, "-") == 0 ? "" : key);
            copy_text(entry.value, RAFT_MAX_VALUE, strcmp(value, "-") == 0 ? "" : value);
            handle_append_entries(node, term, id, a, b, c, d, &entry);
        }
        return;
    }
    if (strcmp(type, "AER") == 0) {
        if (sscanf(line, "AER %u %u %u %u", &term, &id, &a, &b) == 4) {
            handle_append_response(node, term, id, a, b);
        }
        return;
    }
    if (strcmp(type, "JOIN") == 0) {
        memset(host, 0, sizeof(host));
        if (sscanf(line, "JOIN %u %63s %u", &id, host, &a) == 3 && a <= 65535u) {
            handle_join(node, id, host, (uint16_t)a);
        }
        return;
    }
    if (strcmp(type, "PEER") == 0) {
        memset(host, 0, sizeof(host));
        if (sscanf(line, "PEER %u %63s %u", &id, host, &a) == 3 && a <= 65535u) {
            handle_peer_announce(node, id, host, (uint16_t)a);
        }
        return;
    }
    if (strcmp(type, "LEAVE") == 0) {
        if (sscanf(line, "LEAVE %u", &id) == 1) {
            handle_leave(node, id);
        }
        return;
    }
    if (strcmp(type, "CL") == 0) {
        memset(op_text, 0, sizeof(op_text));
        memset(key, 0, sizeof(key));
        memset(value, 0, sizeof(value));
        if (sscanf(line, "CL %u %15s %63s %255s", &id, op_text, key, value) >= 3) {
            handle_client_forward(node, id, op_from_name(op_text), key, strcmp(value, "-") == 0 ? "" : value);
        }
    }
}

void *ticker_loop(void *arg) {
    RaftNode *node = (RaftNode *)arg;
    uint64_t last_heartbeat = 0;
    uint64_t elapsed;
    RaftRole role;
    uint8_t start_vote;

    while (node->running != 0) {
        usleep(50000);
        start_vote = 0;

        pthread_mutex_lock(&node->mutex);
        role = node->role;
        elapsed = now_ms() - node->last_leader_contact_ms;
        if (role != RAFT_LEADER && elapsed > node->election_timeout_ms) {
            start_vote = 1;
        }
        pthread_mutex_unlock(&node->mutex);

        if (start_vote != 0) {
            start_election(node);
            continue;
        }
        if (role == RAFT_LEADER && now_ms() - last_heartbeat > 250u) {
            broadcast_append(node);
            last_heartbeat = now_ms();
        }
    }
    return NULL;
}
