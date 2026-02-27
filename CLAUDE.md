# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

TinyKV is an educational distributed key-value storage system implementing Raft consensus, inspired by MIT 6.824 and TiKV. It's a course project where students progressively build a horizontally scalable, highly available KV store with distributed transaction support.

## Build and Test Commands

```bash
# Build both tinykv-server and tinyscheduler-server
make

# Run all tests
make test

# Run tests for specific projects
make project1              # Standalone KV tests
make project2a             # Raft algorithm tests (all of 2A)
make project2aa            # Leader election tests only
make project2ab            # Log replication tests only
make project2ac            # RawNode interface tests only
make project2b             # Raft KV integration tests
make project2c             # Snapshot handling tests
make project3a             # Raft conf change tests
make project3b             # Multi-raft raftstore tests
make project3c             # Scheduler tests
make project4a             # MVCC layer tests
make project4b             # KvGet/KvPrewrite/KvCommit tests
make project4c             # KvScan/KvCheckTxnStatus/KvBatchRollback/KvResolveLock tests

# Run a single test (example)
go test -v --count=1 --parallel=1 -p=1 ./raft -run TestLeaderElection2AA

# Format code
make format

# Regenerate protobuf files (if proto definitions change)
make proto
```

Set `LOG_LEVEL=debug` environment variable for verbose logging during tests.

## Architecture

### Directory Structure
- **`raft/`**: Core Raft consensus implementation (leader election, log replication, snapshots)
- **`kv/`**: Key-value store implementation
  - `kv/raftstore/`: Raft-based storage layer integrating Raft with the KV store
  - `kv/storage/standalone_storage/`: Single-node storage engine (Project 1)
  - `kv/storage/raft_storage/`: Distributed storage using Raft (Projects 2-3)
  - `kv/transaction/`: MVCC and transaction handling (Project 4)
  - `kv/server/`: gRPC service handlers
- **`scheduler/`**: TinyScheduler for cluster management and load balancing
- **`proto/`**: Protocol Buffer definitions for all RPC communication

### Core Concepts

**Store, Peer, Region**: A Store is a tinykv-server instance. A Peer is a Raft node running on a Store. A Region is a Raft group (collection of Peers) responsible for a key range.

**Two Storage Engines**: TinyKV uses two badger instances:
- `raftdb`: Stores Raft log entries and `RaftLocalState`
- `kvdb`: Stores user data (with column families: default, lock, write) plus `RaftApplyState` and `RegionLocalState`

**Column Families**: Keys are prefixed to simulate CFs in badger (e.g., `${cf}_${key}`). Use `engine_util` package for all CF operations.

### Raft Implementation (`raft/`)
- `raft.go`: Core Raft state machine with `tick()` for timeouts, `Step()` for message handling
- `log.go`: `RaftLog` manages log entries and interacts with Storage
- `rawnode.go`: `RawNode` wraps Raft and provides the `Ready()` interface for the upper layer
- Uses logical ticks instead of physical time; upper layer calls `Tick()` periodically

### Raftstore Flow (`kv/raftstore/`)
1. Client request arrives via gRPC
2. `RaftStorage` sends `RaftCmdRequest` to raftstore via channel
3. `raftWorker` receives request, proposes to Raft
4. After Raft commits, `HandleRaftReady` applies to state machine
5. Response returned via callback

### Transaction Layer (`kv/transaction/`)
- Implements Percolator-style 2PC transactions
- `MvccTxn` handles multi-version concurrency control
- Three CFs: `default` (values), `lock` (locks), `write` (commit records)
- Keys are encoded with timestamps for versioning

## Key Implementation Files

| Project | Key Files to Modify |
|---------|---------------------|
| 1 | `kv/storage/standalone_storage/standalone_storage.go`, `kv/server/raw_api.go` |
| 2A | `raft/raft.go`, `raft/log.go`, `raft/rawnode.go` |
| 2B | `kv/raftstore/peer_storage.go`, `kv/raftstore/peer_msg_handler.go` |
| 2C | `raft/raft.go` (snapshot), `kv/raftstore/peer_msg_handler.go` |
| 3A | `raft/raft.go` (conf change, leader transfer), `raft/rawnode.go` |
| 3B | `kv/raftstore/peer_msg_handler.go`, `kv/raftstore/peer.go` |
| 3C | `scheduler/server/cluster.go`, `scheduler/server/schedulers/balance_region.go` |
| 4A | `kv/transaction/mvcc/transaction.go`, `kv/transaction/mvcc/scanner.go` |
| 4B/C | `kv/server/server.go` |

## Important Notes

- Uses forked badger: `github.com/Connor1996/badger` (not the original dgraph-io version)
- Raft initial log term and index are 5 (not 0) to distinguish from passively created peers
- Tests assume newly elected leader appends a noop entry on its term
- Tests assume first raft start has term 0
- Error types are defined in `proto/proto/errorpb.proto` and `kv/raftstore/util/error.go`
