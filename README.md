# kv

A distributed key–value store: the companion code for the book series
**Distributed Systems Simplified: From First Principles to Production Internals** (Chapter 56, Volume 3).

A replicated key–value store built on a from-scratch Raft implementation, with a deterministic
fault-injecting simulator and a linearizability checker. Plain Java 21, no dependencies.

```bash
./build.sh          # compile and run the test suite (50 randomized chaos seeds)
./build.sh 1000     # run more seeds
```

## What is implemented and tested

| Area | Files |
|------|-------|
| Raft: elections, log replication, commit rule, persistence, snapshots, InstallSnapshot, ReadIndex | `src/kmgkv/raft/RaftNode.java` |
| Durable storage: in-memory (for simulation) and file-based with CRCs, torn-tail recovery, atomic snapshot generations | `src/kmgkv/raft/MemoryStorage.java`, `FileStorage.java` |
| KV state machine with client sessions (retry deduplication) and snapshots | `src/kmgkv/kv/KvStateMachine.java` |
| Server glue: writes through the log, reads through ReadIndex, automatic compaction | `src/kmgkv/kv/KvServer.java` |
| Deterministic simulator: delay, loss, duplication, reordering, partitions, crash/restart, invariant checks | `src/kmgkv/sim/Cluster.java`, `SimClient.java` |
| Linearizability checker (Wing–Gong with memoization, per key) | `src/kmgkv/check/Linearizability.java` |
| Test suite (12 tests, including regression tests for classic Raft bugs) | `src/kmgkv/test/TestMain.java` |

## What is deliberately NOT implemented

Membership changes, sharding and shard migration, pre-vote and check-quorum, leader leases,
chunked snapshot transfer, batching/pipelining, session expiry, a network RPC layer, and an
on-disk state-machine engine. Chapter 56 describes how each would be added and why it is hard.

## Assumptions behind the guarantees

- Crash-stop failures only (no Byzantine faults, no silent disk corruption beyond what CRCs detect).
- A write is durable once `Storage` returns; `FileStorage` makes that true by calling `fsync`,
  which in turn relies on the disk honoring flush requests.
- Retry deduplication assumes each client has at most one outstanding write and retries it with
  the same sequence number (see `KvStateMachine`).

This is teaching code. Use etcd's raft, hashicorp/raft, or Apache Ratis for production systems.

## Using this repository with the book

- Chapter 56 explains the design, and its code listings are excerpts from the files in `src/kmgkv/`.
  Appendix A.1 in Volume 3 describes this repository.
- [`READING-GUIDE.md`](READING-GUIDE.md) is the detailed, file-by-file guide to the source code of the
  open-source systems discussed in each chapter. It lives here rather than in the printed book because
  repository layouts change; it is updated as the projects evolve.

## License

Apache License 2.0; see [`LICENSE`](LICENSE).
