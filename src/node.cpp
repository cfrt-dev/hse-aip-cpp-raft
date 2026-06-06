#include "raft_internal.hpp"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

static void print_state(RaftNode *node) {
    pthread_mutex_lock(&node->mutex);
    printf("node=%u role=%s term=%u leader=%d peers=%u commit=%u applied=%u log=%u\n", node->id,
           role_name(node->role), node->current_term, node->leader_id, node->peer_count, node->commit_index,
           node->last_applied, node->log_count);
    pthread_mutex_unlock(&node->mutex);
}

static void print_store(RaftNode *node) {
    uint32_t i;
    pthread_mutex_lock(&node->mutex);
    printf("store contains %u item(s)\n", node->store.count);
    for (i = 0; i < RAFT_MAX_LOG; ++i) {
        if (node->store.items[i].used != 0) {
            printf("%s=%s\n", node->store.items[i].key, node->store.items[i].value);
        }
    }
    pthread_mutex_unlock(&node->mutex);
}

void *stdin_loop(void *arg) {
    RaftNode *node = (RaftNode *)arg;
    char line[RAFT_MAX_LINE];
    char key[RAFT_MAX_KEY];
    char value[RAFT_MAX_VALUE];
    KvOp op;
    int32_t rc;

    while (node->running != 0 && fgets(line, sizeof(line), stdin) != NULL) {
        line[strcspn(line, "\r\n")] = '\0';
        if (strcmp(line, "quit") == 0 || strcmp(line, "exit") == 0) {
            raft_node_stop(node);
            break;
        }
        if (strcmp(line, "state") == 0) {
            print_state(node);
            continue;
        }
        if (strcmp(line, "show") == 0) {
            print_store(node);
            continue;
        }
        memset(key, 0, sizeof(key));
        memset(value, 0, sizeof(value));
        rc = parse_kv_command(line, &op, key, value);
        if (rc != 0) {
            printf("bad command\n");
            continue;
        }
        if (op == KV_OP_NONE) {
            pthread_mutex_lock(&node->mutex);
            rc = kv_get(&node->store, key, value, sizeof(value));
            pthread_mutex_unlock(&node->mutex);
            if (rc == 0) {
                printf("%s=%s\n", key, value);
            } else {
                printf("%s is not found\n", key);
            }
        } else {
            raft_submit_command(node, op, key, value);
        }
    }
    return NULL;
}

int32_t raft_config_init(RaftConfig *config) {
    if (config == NULL) {
        return -1;
    }
    memset(config, 0, sizeof(*config));
    config->id = 1;
    copy_text(config->host, sizeof(config->host), "127.0.0.1");
    config->port = 9001;
    copy_text(config->log_path, sizeof(config->log_path), "logs/node1.log");
    config->workers = 4;
    return 0;
}

static int32_t parse_peer(Peer *peer, const char *text) {
    unsigned int id;
    unsigned int port;
    char host[64];

    memset(host, 0, sizeof(host));
    if (sscanf(text, "%u:%63[^:]:%u", &id, host, &port) != 3) {
        return -1;
    }
    if (id == 0 || port > 65535u) {
        return -2;
    }
    memset(peer, 0, sizeof(*peer));
    peer->id = id;
    copy_text(peer->host, sizeof(peer->host), host);
    peer->port = (uint16_t)port;
    peer->next_index = 1;
    peer->match_index = 0;
    return 0;
}

int32_t raft_config_parse(RaftConfig *config, int argc, char **argv) {
    int i;
    if (config == NULL || argv == NULL) {
        return -1;
    }
    for (i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--id") == 0 && i + 1 < argc) {
            config->id = (uint32_t)strtoul(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--host") == 0 && i + 1 < argc) {
            copy_text(config->host, sizeof(config->host), argv[++i]);
        } else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            config->port = (uint16_t)strtoul(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--peer") == 0 && i + 1 < argc) {
            if (config->peer_count >= RAFT_MAX_PEERS ||
                parse_peer(&config->peers[config->peer_count], argv[++i]) != 0) {
                return -2;
            }
            config->peer_count++;
        } else if (strcmp(argv[i], "--log") == 0 && i + 1 < argc) {
            copy_text(config->log_path, sizeof(config->log_path), argv[++i]);
        } else if (strcmp(argv[i], "--workers") == 0 && i + 1 < argc) {
            config->workers = (uint32_t)strtoul(argv[++i], NULL, 10);
        } else {
            return -3;
        }
    }
    if (config->id == 0 || config->port == 0) {
        return -4;
    }
    return 0;
}

void raft_config_print_usage(const char *program) {
    printf("usage: %s --id N --port PORT --peer ID:HOST:PORT [--peer ...] [--log PATH]\n", program);
    printf("example: %s --id 1 --port 9001 --peer 2:127.0.0.1:9002 --peer 3:127.0.0.1:9003\n", program);
}

int32_t raft_node_init(RaftNode *node, const RaftConfig *config) {
    uint32_t i;
    if (node == NULL || config == NULL) {
        return -1;
    }
    memset(node, 0, sizeof(*node));
    node->id = config->id;
    copy_text(node->host, sizeof(node->host), config->host);
    node->port = config->port;
    node->peer_count = config->peer_count;
    for (i = 0; i < config->peer_count; ++i) {
        node->peers[i] = config->peers[i];
    }
    copy_text(node->log_path, sizeof(node->log_path), config->log_path);
    node->role = RAFT_FOLLOWER;
    node->voted_for = -1;
    node->leader_id = -1;
    node->server_fd = -1;
    node->epoll_fd = -1;
    node->last_leader_contact_ms = now_ms();
    node->election_timeout_ms = random_election_timeout(node);
    if (pthread_mutex_init(&node->mutex, NULL) != 0) {
        return -2;
    }
    open_event_log(node);
    if (pool_init(&node->pool, config->workers) != 0) {
        return -3;
    }
    return 0;
}

int32_t raft_node_start(RaftNode *node) {
    if (node == NULL) {
        return -1;
    }
    if (create_server(node) != 0) {
        return -2;
    }
    node->running = 1;
    if (pthread_create(&node->network_thread, NULL, network_loop, node) != 0) {
        return -3;
    }
    if (pthread_create(&node->ticker_thread, NULL, ticker_loop, node) != 0) {
        return -4;
    }
    if (pthread_create(&node->stdin_thread, NULL, stdin_loop, node) != 0) {
        return -5;
    }
    log_event(node, "started on %s:%u with %u peer(s)", node->host, node->port, node->peer_count);
    request_cluster_join(node);
    return 0;
}

void raft_node_stop(RaftNode *node) {
    if (node == NULL) {
        return;
    }
    if (node->running != 0) {
        request_cluster_leave(node);
    }
    node->running = 0;
    if (node->server_fd >= 0) {
        shutdown(node->server_fd, SHUT_RDWR);
    }
}

void raft_node_destroy(RaftNode *node) {
    uint32_t i;
    if (node == NULL) {
        return;
    }
    node->running = 0;
    if (node->network_thread != 0) {
        pthread_join(node->network_thread, NULL);
    }
    if (node->ticker_thread != 0) {
        pthread_join(node->ticker_thread, NULL);
    }
    if (node->stdin_thread != 0) {
        pthread_cancel(node->stdin_thread);
        pthread_join(node->stdin_thread, NULL);
    }
    pool_destroy(&node->pool);
    for (i = 0; i < RAFT_MAX_CONN; ++i) {
        if (node->connections[i] != NULL) {
            close_connection(node, (int32_t)i);
        }
    }
    if (node->server_fd >= 0) {
        close(node->server_fd);
        node->server_fd = -1;
    }
    if (node->epoll_fd >= 0) {
        close(node->epoll_fd);
        node->epoll_fd = -1;
    }
    if (node->event_log != NULL) {
        fclose(node->event_log);
        node->event_log = NULL;
    }
    pthread_mutex_destroy(&node->mutex);
}
