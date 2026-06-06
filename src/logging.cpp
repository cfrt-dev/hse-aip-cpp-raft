#include "raft_internal.h"

#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>

void log_event(RaftNode *node, const char *format, ...) {
    va_list args;
    uint64_t ts = now_ms();

    if (node->event_log == NULL) {
        return;
    }

    fprintf(node->event_log, "[%llu] node %u ", (unsigned long long)ts, node->id);
    va_start(args, format);
    vfprintf(node->event_log, format, args);
    va_end(args);
    fprintf(node->event_log, "\n");
    fflush(node->event_log);
}

void open_event_log(RaftNode *node) {
    char dir[RAFT_MAX_PATH];
    copy_text(dir, sizeof(dir), node->log_path);

    char *slash = strrchr(dir, '/');
    if (slash != NULL) {
        *slash = '\0';
        mkdir(dir, 0755);
    }
    node->event_log = fopen(node->log_path, "a");
}
