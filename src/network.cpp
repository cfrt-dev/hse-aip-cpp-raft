#include "raft_internal.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

int32_t connect_send(const char *host, uint16_t port, const char *line) {
    int32_t fd;
    int32_t rc;
    int32_t err;
    socklen_t err_len;
    struct sockaddr_in addr;
    struct pollfd pfd;
    size_t length;
    ssize_t sent;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    set_nonblock(fd);

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
        close(fd);
        return -2;
    }

    rc = connect(fd, (struct sockaddr *)&addr, sizeof(addr));
    if (rc != 0 && errno != EINPROGRESS) {
        close(fd);
        return -3;
    }
    if (rc != 0) {
        memset(&pfd, 0, sizeof(pfd));
        pfd.fd = fd;
        pfd.events = POLLOUT;
        if (poll(&pfd, 1, 250) <= 0) {
            close(fd);
            return -4;
        }
        err = 0;
        err_len = sizeof(err);
        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &err_len) != 0 || err != 0) {
            close(fd);
            return -5;
        }
    }

    length = strlen(line);
    sent = send(fd, line, length, MSG_NOSIGNAL);
    if (sent < 0 || (size_t)sent != length) {
        close(fd);
        return -6;
    }
    close(fd);
    return 0;
}

int32_t send_to_peer(RaftNode *node, uint32_t peer_id, const char *line) {
    Peer *peer = find_peer(node, peer_id);
    if (peer == NULL) {
        return -1;
    }
    return connect_send(peer->host, peer->port, line);
}

static int32_t build_append_locked(RaftNode *node, Peer *peer, char *out, uint32_t out_size) {
    uint32_t next_index;
    uint32_t prev_index;
    uint32_t prev_term;
    RaftLogEntry *entry;

    if (peer->next_index <= 0) {
        peer->next_index = (int32_t)node->log_count + 1;
    }
    next_index = (uint32_t)peer->next_index;
    if (next_index == 0) {
        next_index = 1;
    }
    if (next_index <= node->log_count) {
        entry = &node->log[next_index - 1u];
        prev_index = entry->index - 1u;
        prev_term = prev_index == 0 ? 0 : node->log[prev_index - 1u].term;
        snprintf(out, out_size, "AE %u %u %u %u %u 1 %u %u %s %s %s\n", node->current_term, node->id, prev_index,
                 prev_term, node->commit_index, entry->index, entry->term, op_name(entry->op), entry->key,
                 entry->value[0] == '\0' ? "-" : entry->value);
        return 0;
    }

    prev_index = node->log_count;
    prev_term = prev_index == 0 ? 0 : node->log[prev_index - 1u].term;
    snprintf(out, out_size, "AE %u %u %u %u %u 0 0 0 NONE - -\n", node->current_term, node->id, prev_index, prev_term,
             node->commit_index);
    return 0;
}

void send_append_to_peer(RaftNode *node, uint32_t peer_id) {
    char line[RAFT_MAX_LINE];
    Peer peer_copy;
    int32_t found = 0;
    uint32_t i;

    memset(line, 0, sizeof(line));
    memset(&peer_copy, 0, sizeof(peer_copy));
    pthread_mutex_lock(&node->mutex);
    for (i = 0; i < node->peer_count; ++i) {
        if (node->peers[i].id == peer_id) {
            build_append_locked(node, &node->peers[i], line, sizeof(line));
            peer_copy = node->peers[i];
            found = 1;
            break;
        }
    }
    pthread_mutex_unlock(&node->mutex);

    if (found != 0 && connect_send(peer_copy.host, peer_copy.port, line) != 0) {
        log_event(node, "cannot reach peer %u", peer_id);
    }
}

void broadcast_append(RaftNode *node) {
    uint32_t ids[RAFT_MAX_PEERS];
    uint32_t count;
    uint32_t i;

    count = 0;
    pthread_mutex_lock(&node->mutex);
    for (i = 0; i < node->peer_count; ++i) {
        ids[count++] = node->peers[i].id;
    }
    pthread_mutex_unlock(&node->mutex);

    for (i = 0; i < count; ++i) {
        send_append_to_peer(node, ids[i]);
    }
}

static void *pool_worker(void *arg) {
    ThreadPool *pool = (ThreadPool *)arg;
    ThreadJob *job;

    while (1) {
        pthread_mutex_lock(&pool->mutex);
        while (pool->head == NULL && pool->stop == 0) {
            pthread_cond_wait(&pool->cond, &pool->mutex);
        }
        if (pool->stop != 0 && pool->head == NULL) {
            pthread_mutex_unlock(&pool->mutex);
            break;
        }
        job = pool->head;
        pool->head = job->next;
        if (pool->head == NULL) {
            pool->tail = NULL;
        }
        pthread_mutex_unlock(&pool->mutex);

        raft_handle_line(job->node, job->line);
        free(job);
    }
    return NULL;
}

int32_t pool_init(ThreadPool *pool, uint32_t thread_count) {
    uint32_t i;
    memset(pool, 0, sizeof(*pool));
    if (thread_count == 0) {
        thread_count = 4;
    }
    pool->thread_count = thread_count;
    if (pthread_mutex_init(&pool->mutex, NULL) != 0 || pthread_cond_init(&pool->cond, NULL) != 0) {
        return -1;
    }
    pool->threads = (pthread_t *)calloc(thread_count, sizeof(pthread_t));
    if (pool->threads == NULL) {
        return -2;
    }
    for (i = 0; i < thread_count; ++i) {
        if (pthread_create(&pool->threads[i], NULL, pool_worker, pool) != 0) {
            return -3;
        }
    }
    return 0;
}

int32_t pool_submit(ThreadPool *pool, RaftNode *node, const char *line) {
    ThreadJob *job = (ThreadJob *)calloc(1, sizeof(ThreadJob));
    if (job == NULL) {
        return -1;
    }
    copy_text(job->line, RAFT_MAX_LINE, line);
    job->node = node;

    pthread_mutex_lock(&pool->mutex);
    if (pool->tail == NULL) {
        pool->head = job;
        pool->tail = job;
    } else {
        pool->tail->next = job;
        pool->tail = job;
    }
    pthread_cond_signal(&pool->cond);
    pthread_mutex_unlock(&pool->mutex);
    return 0;
}

void pool_destroy(ThreadPool *pool) {
    uint32_t i;
    ThreadJob *job;
    ThreadJob *next;

    if (pool == NULL) {
        return;
    }
    pthread_mutex_lock(&pool->mutex);
    pool->stop = 1;
    pthread_cond_broadcast(&pool->cond);
    pthread_mutex_unlock(&pool->mutex);

    for (i = 0; i < pool->thread_count; ++i) {
        pthread_join(pool->threads[i], NULL);
    }
    free(pool->threads);
    pool->threads = NULL;

    job = pool->head;
    while (job != NULL) {
        next = job->next;
        free(job);
        job = next;
    }
    pthread_mutex_destroy(&pool->mutex);
    pthread_cond_destroy(&pool->cond);
}

int32_t create_server(RaftNode *node) {
    int32_t fd;
    int32_t opt = 1;
    struct sockaddr_in addr;
    struct epoll_event event;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    if (set_nonblock(fd) != 0) {
        close(fd);
        return -2;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(node->port);
    if (inet_pton(AF_INET, node->host, &addr.sin_addr) != 1) {
        close(fd);
        return -3;
    }
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -4;
    }
    if (listen(fd, 128) != 0) {
        close(fd);
        return -5;
    }

    node->epoll_fd = epoll_create1(0);
    if (node->epoll_fd < 0) {
        close(fd);
        return -6;
    }

    memset(&event, 0, sizeof(event));
    event.events = EPOLLIN;
    event.data.fd = fd;
    if (epoll_ctl(node->epoll_fd, EPOLL_CTL_ADD, fd, &event) != 0) {
        close(fd);
        close(node->epoll_fd);
        return -7;
    }
    node->server_fd = fd;
    return 0;
}

void close_connection(RaftNode *node, int32_t fd) {
    if (fd < 0 || fd >= RAFT_MAX_CONN) {
        return;
    }
    epoll_ctl(node->epoll_fd, EPOLL_CTL_DEL, fd, NULL);
    if (node->connections[fd] != NULL) {
        free(node->connections[fd]);
        node->connections[fd] = NULL;
    }
    close(fd);
}

static void accept_connections(RaftNode *node) {
    int32_t fd;
    struct sockaddr_in addr;
    socklen_t len;
    struct epoll_event event;
    Connection *connection;

    while (1) {
        len = sizeof(addr);
        fd = accept(node->server_fd, (struct sockaddr *)&addr, &len);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return;
            }
            return;
        }
        if (fd >= RAFT_MAX_CONN || set_nonblock(fd) != 0) {
            close(fd);
            continue;
        }
        connection = (Connection *)calloc(1, sizeof(Connection));
        if (connection == NULL) {
            close(fd);
            continue;
        }
        connection->fd = fd;
        node->connections[fd] = connection;

        memset(&event, 0, sizeof(event));
        event.events = EPOLLIN | EPOLLRDHUP;
        event.data.fd = fd;
        if (epoll_ctl(node->epoll_fd, EPOLL_CTL_ADD, fd, &event) != 0) {
            close_connection(node, fd);
        }
    }
}

static void dispatch_complete_lines(RaftNode *node, Connection *connection) {
    uint32_t start = 0;
    uint32_t i;
    uint32_t line_len;
    char line[RAFT_MAX_LINE];

    for (i = 0; i < connection->length; ++i) {
        if (connection->buffer[i] != '\n') {
            continue;
        }
        line_len = i - start;
        if (line_len >= RAFT_MAX_LINE) {
            line_len = RAFT_MAX_LINE - 1u;
        }
        memcpy(line, connection->buffer + start, line_len);
        line[line_len] = '\0';
        if (line_len > 0 && line[line_len - 1u] == '\r') {
            line[line_len - 1u] = '\0';
        }
        pool_submit(&node->pool, node, line);
        start = i + 1u;
    }

    if (start > 0) {
        memmove(connection->buffer, connection->buffer + start, connection->length - start);
        connection->length -= start;
    }
}

static void read_connection(RaftNode *node, int32_t fd) {
    Connection *connection;
    ssize_t bytes;
    uint32_t remaining;

    if (fd < 0 || fd >= RAFT_MAX_CONN || node->connections[fd] == NULL) {
        return;
    }
    connection = node->connections[fd];
    while (1) {
        remaining = (uint32_t)sizeof(connection->buffer) - connection->length - 1u;
        if (remaining == 0) {
            close_connection(node, fd);
            return;
        }
        bytes = recv(fd, connection->buffer + connection->length, remaining, 0);
        if (bytes > 0) {
            connection->length += (uint32_t)bytes;
            connection->buffer[connection->length] = '\0';
            dispatch_complete_lines(node, connection);
            continue;
        }
        if (bytes == 0) {
            close_connection(node, fd);
            return;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return;
        }
        close_connection(node, fd);
        return;
    }
}

void *network_loop(void *arg) {
    RaftNode *node = (RaftNode *)arg;
    struct epoll_event events[64];
    int32_t count;
    int32_t i;
    int32_t fd;

    while (node->running != 0) {
        count = epoll_wait(node->epoll_fd, events, 64, 200);
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        for (i = 0; i < count; ++i) {
            fd = events[i].data.fd;
            if (fd == node->server_fd) {
                accept_connections(node);
                continue;
            }
            if ((events[i].events & EPOLLIN) != 0) {
                read_connection(node, fd);
            }
            if (fd >= 0 && fd < RAFT_MAX_CONN && node->connections[fd] != NULL &&
                (events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0) {
                close_connection(node, fd);
            }
        }
    }
    return NULL;
}
