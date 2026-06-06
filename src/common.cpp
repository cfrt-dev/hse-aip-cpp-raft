#include "raft_internal.h"

#include <ctype.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

uint64_t now_ms(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
}

int32_t set_nonblock(int32_t fd) {
    int32_t flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return -1;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
        return -1;
    }
    return 0;
}

void copy_text(char *dst, uint32_t dst_size, const char *src) {
    if (dst == NULL || dst_size == 0) {
        return;
    }
    if (src == NULL) {
        dst[0] = '\0';
        return;
    }
    snprintf(dst, dst_size, "%s", src);
}

int32_t is_valid_token(const char *text, uint32_t max_size) {
    uint32_t i;
    if (text == NULL || text[0] == '\0') {
        return 0;
    }
    for (i = 0; text[i] != '\0'; ++i) {
        if (i + 1 >= max_size || isspace((unsigned char)text[i])) {
            return 0;
        }
    }
    return 1;
}

const char *role_name(RaftRole role) {
    if (role == RAFT_LEADER) {
        return "leader";
    }
    if (role == RAFT_CANDIDATE) {
        return "candidate";
    }
    return "follower";
}

const char *op_name(KvOp op) {
    if (op == KV_OP_SET) {
        return "SET";
    }
    if (op == KV_OP_DEL) {
        return "DEL";
    }
    return "NONE";
}

KvOp op_from_name(const char *name) {
    if (strcmp(name, "SET") == 0) {
        return KV_OP_SET;
    }
    if (strcmp(name, "DEL") == 0) {
        return KV_OP_DEL;
    }
    return KV_OP_NONE;
}

Peer *find_peer(RaftNode *node, uint32_t id) {
    for (uint32_t i = 0; i < node->peer_count; ++i) {
        if (node->peers[i].id == id) {
            return &node->peers[i];
        }
    }
    return NULL;
}

int32_t add_peer_locked(RaftNode *node, uint32_t id, const char *host, uint16_t port) {
    if (node == NULL || id == 0 || id == node->id || host == NULL || host[0] == '\0' || port == 0) {
        return -1;
    }

    Peer *peer = find_peer(node, id);
    if (peer != NULL) {
        copy_text(peer->host, sizeof(peer->host), host);
        peer->port = port;
        return 0;
    }
    if (node->peer_count >= RAFT_MAX_PEERS) {
        return -2;
    }

    peer = &node->peers[node->peer_count];
    memset(peer, 0, sizeof(*peer));
    peer->id = id;
    copy_text(peer->host, sizeof(peer->host), host);
    peer->port = port;
    peer->next_index = (int32_t)node->log_count + 1;
    peer->match_index = 0;
    node->peer_count++;
    return 1;
}

int32_t remove_peer_locked(RaftNode *node, uint32_t id) {
    if (node == NULL || id == 0 || id == node->id) {
        return -1;
    }

    for (uint32_t i = 0; i < node->peer_count; ++i) {
        if (node->peers[i].id != id) {
            continue;
        }
        for (uint32_t j = i + 1u; j < node->peer_count; ++j) {
            node->peers[j - 1u] = node->peers[j];
        }
        if (node->peer_count > 0) {
            node->peer_count--;
            memset(&node->peers[node->peer_count], 0, sizeof(node->peers[node->peer_count]));
        }
        if (node->voted_for == (int32_t)id) {
            node->voted_for = -1;
        }
        if (node->leader_id == (int32_t)id) {
            node->leader_id = -1;
            node->last_leader_contact_ms = 0;
        }
        return 1;
    }

    return 0;
}

int32_t majority_count(const RaftNode *node) {
    return (int32_t)((node->peer_count + 1u) / 2u) + 1;
}

uint32_t last_log_index_locked(const RaftNode *node) {
    return node->log_count;
}

uint32_t last_log_term_locked(const RaftNode *node) {
    if (node->log_count == 0) {
        return 0;
    }
    return node->log[node->log_count - 1u].term;
}

uint64_t random_election_timeout(const RaftNode *node) {
    uint64_t base = 900u + (uint64_t)(node->id * 137u) % 300u;
    return base + (now_ms() % 500u);
}
