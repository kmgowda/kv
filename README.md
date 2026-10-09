# kv

A distributed key–value store: the companion code for the book series
**Distributed Systems Simplified: From First Principles to Production Internals** (Chapter 56, Volume 3).

A replicated key–value store built on a from-scratch Raft implementation, with a deterministic
fault-injecting simulator and a linearizability checker. Plain Java 21, no runtime or test dependencies.
It builds with Gradle; the Gradle wrapper is included, so you only need JDK 21 or newer.

```bash
./gradlew test                 # compile (warnings are errors) and run the 12 tests with 50 chaos seeds
./gradlew test -Pseeds=1000    # the same, with 1,000 randomized chaos seeds
```

On Windows, use `gradlew.bat` instead of `./gradlew`.

## Project layout

```text
kv/
├── build.gradle.kts            build: Java 21, warnings as errors, `test` runs the simulation suite
├── settings.gradle.kts
├── gradlew, gradlew.bat        Gradle wrapper (downloads the pinned Gradle version on first use)
├── gradle/wrapper/
├── src/main/java/kv/
│   ├── raft/                   the Raft core and its storage
│   ├── store/                  the key–value state machine and server
│   ├── sim/                    the deterministic fault-injecting simulator
│   └── check/                  the linearizability checker
├── src/test/java/kv/           the test suite
└── READING-GUIDE.md            file-level reading guide to the open-source systems in the book
```

## What is implemented and tested

| Area | Files (under `src/main/java/kv/` unless shown) |
|------|-------|
| Raft: elections, log replication, commit rule, persistence, snapshots, InstallSnapshot, ReadIndex | `raft/RaftNode.java` |
| Durable storage: in-memory (for simulation) and file-based with CRCs, torn-tail recovery, atomic snapshot generations | `raft/MemoryStorage.java`, `raft/FileStorage.java` |
| KV state machine with client sessions (retry deduplication) and snapshots | `store/KvStateMachine.java` |
| Server glue: writes through the log, reads through ReadIndex, automatic compaction | `store/KvServer.java` |
| Deterministic simulator: delay, loss, duplication, reordering, partitions, crash/restart, invariant checks | `sim/Cluster.java`, `sim/SimClient.java` |
| Linearizability checker (Wing–Gong with memoization, per key) | `check/Linearizability.java` |
| Test suite (12 tests, including regression tests for classic Raft bugs) | `src/test/java/kv/TestMain.java` |

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

- Chapter 56 explains the design, and its code listings are excerpts from the files in `src/main/java/kv/`.
  Appendix A.1 in Volume 3 describes this repository.
- [`READING-GUIDE.md`](READING-GUIDE.md) is the detailed, file-by-file guide to the source code of the
  open-source systems discussed in each chapter. It lives here rather than in the printed book because
  repository layouts change; it is updated as the projects evolve.

## License

Apache License 2.0; see [`LICENSE`](LICENSE).
