# RAFT KV

`raft_kv` is a small console key-value database that demonstrates the RAFT consensus algorithm. Several instances can be
started in separate terminals. They elect a leader, replicate `set` and `del` commands, and keep an operational log for
each node.

The implementation is intentionally C-style even though files use the `.cpp` extension: fixed-size structs, integer
return values, manual memory management, `pthread` workers, and an `epoll` TCP accept loop.

## Build

```sh
cc -o nob nob.c
./nob
./nob test
```

## Three-node demo

Terminal 1:

```sh
./build/raft_kv --id 1 --port 9001 --log logs/node1.log
```

Terminal 2:

```sh
./build/raft_kv --id 2 --port 9002 --log logs/node2.log --peer 1:127.0.0.1:9001
```

The contacted node sends its known peer list back to node 4 and announces node 4 to the rest of the cluster. Use `state`
on each terminal to check that every node eventually shows `peers=3` in a four-node cluster.

Useful commands inside any node terminal:

```text
state
set color blue
get color
del color
show
quit
```

Writes entered on a follower are forwarded to the known leader. If the current leader is stopped with `Ctrl+C`, the
remaining majority elects a new leader after the election timeout. The per-node files in `logs/` show elections,
replication, commits, and applied state-machine commands.

`quit` and `exit` perform a graceful leave: the node sends `LEAVE` to known peers, and the remaining nodes remove it from
their peer lists. This reduces the majority size for future commits. If a process is killed without this message, peers
keep it in membership and treat it as unavailable.

## Network behavior

The network layer uses a TCP listener registered in `epoll`. Accepted messages are copied into a `pthread` worker pool,
where RAFT messages are parsed and applied under the node mutex. Outgoing peer messages use short-lived TCP connections,
which makes manual availability experiments easy: stopping an instance or blocking its port immediately appears in the
replication log without keeping stale persistent sockets.

## Source layout

- `src/common.cpp` - small shared helpers for time, strings, roles, operations and peer lookup.
- `src/kv_store.cpp` - key-value store operations and console command parsing.
- `src/logging.cpp` - per-node event log handling.
- `src/network.cpp` - outgoing TCP sends, `epoll` server, connection buffers and `pthread` worker pool.
- `src/raft_core.cpp` - RAFT elections, vote handling, log replication, commits and client command forwarding.
- `src/node.cpp` - config parsing, node lifecycle and interactive stdin commands.
- `src/raft_internal.h` - private cross-module declarations.

## Protocol

The peer protocol is line based:

```text
JOIN <node_id> <host> <port>
PEER <node_id> <host> <port>
LEAVE <node_id>
RV  <term> <candidate_id> <last_log_index> <last_log_term>
RVR <term> <voter_id> <granted>
AE  <term> <leader_id> <prev_index> <prev_term> <leader_commit> <has_entry> <entry_index> <entry_term> <op> <key> <value>
AER <term> <peer_id> <success> <match_index>
CL  <source_id> <op> <key> <value>
```

Keys and values are single tokens without spaces. This keeps the demo protocol readable in logs and simple to inspect.
