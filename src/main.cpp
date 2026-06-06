#include "raft_kv.hpp"

#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static RaftNode *g_node = NULL;

static void handle_signal(int signal_number) {
    (void)signal_number;
    if (g_node != NULL) {
        raft_node_stop(g_node);
    }
}

int main(int argc, char **argv) {
    RaftConfig config;
    RaftNode node;

    if (raft_config_init(&config) != 0) {
        fprintf(stderr, "cannot initialize config\n");
        return 1;
    }

    if (raft_config_parse(&config, argc, argv) != 0) {
        raft_config_print_usage(argv[0]);
        return 1;
    }

    if (raft_node_init(&node, &config) != 0) {
        fprintf(stderr, "cannot initialize node\n");
        return 1;
    }

    g_node = &node;
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    if (raft_node_start(&node) != 0) {
        fprintf(stderr, "cannot start node\n");
        raft_node_destroy(&node);
        return 1;
    }

    printf("node %u is running on %s:%u\n", node.id, node.host, node.port);
    printf("commands: set <key> <value>, get <key>, del <key>, show, state, quit\n");

    while (node.running != 0) {
        sleep(1);
    }

    raft_node_destroy(&node);
    return 0;
}
