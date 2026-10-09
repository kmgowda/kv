# Source Code Reading Guide

*Companion to Distributed Systems Simplified — Appendix A. This file is not part of the printed book and is updated as the projects change.*

For each chapter, this guide lists where to find the code that implements the ideas described in the chapter, down to directories and files. Repository layouts change between releases: if a path no longer exists, search the repository for the type or function name, or for the concept itself.

*Last checked against the projects in October 2026.*

## Chapter 1: Introduction to Distributed Systems

etcd is written in Go. Its repository (github.com/etcd-io/etcd) is organized roughly as follows (directory names can change between versions, so treat this as a map, not a contract):

| Directory | What it contains |
|-----------|------------------|
| `server/etcdserver/` | The main server logic: receives requests, proposes them to Raft, applies committed entries |
| `raft` (now the separate `go.etcd.io/raft` module) | The Raft consensus library, designed as a pure state machine with no networking or disk code |
| `server/storage/wal/` | The write-ahead log implementation |
| `server/storage/mvcc/` | The multi-version key-value store |
| `server/etcdserver/api/rafthttp/` | Peer-to-peer transport between members |
| `client/v3/` | The Go client library |

## Chapter 2: Computer Network Basics

In the Linux kernel source tree (`net/ipv4/`):

| File | Responsibility |
|------|----------------|
| `tcp.c` | Socket-level operations (sendmsg, recvmsg, setsockopt) |
| `tcp_input.c` | Processing incoming segments: ACK handling, fast retransmit, SACK, window updates |
| `tcp_output.c` | Building and transmitting segments, Nagle logic |
| `tcp_timer.c` | Retransmission, delayed-ACK, keep-alive timers |
| `tcp_ipv4.c` | IPv4-specific glue, connection lookup, SYN handling |
| `tcp_cubic.c`, `tcp_bbr.c` | Congestion control algorithms |
| `ip_input.c`, `ip_output.c` | IP receive and send paths |

A typical receive path: `ip_rcv()` → `ip_local_deliver()` → `tcp_v4_rcv()` (finds socket) → `tcp_rcv_established()` (fast path for in-order data on established connections) → data queued to the socket → application woken up.

## Chapter 3: Processes and Threads — Computer Architecture and Operating System Foundations

PostgreSQL source (`src/backend/` in the postgres repository):

| Path | Responsibility |
|------|----------------|
| `postmaster/postmaster.c` | Main supervisor loop: accept connections, fork backends, restart helpers |
| `tcop/postgres.c` | Main loop of a backend: read query, parse, plan, execute ("traffic cop") |
| `storage/buffer/` | Shared buffer manager |
| `storage/lmgr/` | Lock manager |
| `access/transam/xlog.c` | Write-ahead log (WAL) |
| `replication/walsender.c` | Streaming WAL to replicas |
| `postmaster/checkpointer.c`, `bgwriter.c`, `walwriter.c` | Background helper processes |

Nginx source (`src/` in nginx repository): `core/nginx.c` (startup), `os/unix/ngx_process_cycle.c` (master/worker loops), `event/modules/ngx_epoll_module.c` (epoll event loop).

## Chapter 4: Concurrency and Parallelism

**Redis** (github.com/redis/redis, `src/`):

| File | Responsibility |
|------|----------------|
| `ae.c`, `ae_epoll.c` | Event loop abstraction and epoll backend |
| `networking.c` | Client connections, reading queries, writing replies, I/O threads |
| `server.c` | `main()`, `serverCron`, `processCommand`, `call()` |
| `bio.c` | Background I/O threads |
| `t_string.c`, `t_list.c`, ... | Data type command implementations |

**Go runtime** (github.com/golang/go, `src/runtime/`):

| File | Responsibility |
|------|----------------|
| `proc.go` | Scheduler: `schedule()`, `findRunnable()`, work stealing |
| `runtime2.go` | Definitions of `g`, `m`, `p` structures |
| `netpoll.go`, `netpoll_epoll.go` | Network poller integration |
| `chan.go` | Channel implementation (send, receive, select) |
| `sema.go`, `../sync/mutex.go` | Semaphores and mutex implementation |

## Chapter 5: Application Protocols — HTTP, HTTPS, DNS and Name Services

CoreDNS (github.com/coredns/coredns):

| Path | Responsibility |
|------|----------------|
| `core/dnsserver/` | Server setup, listening, dispatching queries to plugin chains |
| `plugin/` | One directory per plugin (`kubernetes`, `forward`, `cache`, `errors`, ...) |
| `plugin/kubernetes/controller.go` | Watches Kubernetes objects and maintains an indexed cache |
| `plugin/kubernetes/kubernetes.go` | Builds DNS answers for service and pod names |
| `plugin/forward/` | Upstream forwarding with health checks |
| `plugin/cache/` | Positive and negative response cache |

Each plugin implements a `ServeDNS(ctx, writer, msg)` method and calls the next plugin when it does not handle the query — the classic chain-of-responsibility pattern.

## Chapter 6: Remote Communication — Request-Reply, RPC, RMI, REST and gRPC

grpc-go (github.com/grpc/grpc-go):

| Path | Responsibility |
|------|----------------|
| `clientconn.go` | `ClientConn`: dialing, state management, resolver and balancer wiring |
| `resolver/` | Resolver API; `internal/resolver/dns` implementation |
| `balancer/` | Balancer API; `roundrobin`, `pickfirst`, `weightedroundrobin` |
| `internal/transport/http2_client.go`, `http2_server.go` | HTTP/2 transport |
| `server.go` | Server, service registration, stream handling |
| `interceptor.go` | Interceptor types |
| `codes/`, `status/` | Status codes and rich error details |
| `xds/` | xDS-based resolver/balancer for proxyless service mesh |
| `cmd/protoc-gen-go-grpc/` | Code generator for service stubs |

## Chapter 7: Load Balancers, Reverse Proxies and API Gateways

Envoy (github.com/envoyproxy/envoy), C++:

| Path | Responsibility |
|------|----------------|
| `source/server/` | Server startup, main thread, admin, worker management |
| `source/common/event/` | Event loop (libevent) dispatcher abstraction |
| `source/common/http/conn_manager_impl.cc` | HTTP connection manager: stream lifecycle, filter chain |
| `source/common/router/` | Router filter: route matching, retries, timeouts, shadowing |
| `source/common/upstream/` | Cluster manager, load balancers (round robin, least request, ring hash, Maglev), health checking, outlier detection |
| `source/extensions/filters/http/` | HTTP filters (jwt_authn, ratelimit, cors, ext_authz, ...) |
| `source/common/config/` | xDS subscription machinery |
| `api/envoy/` | Protobuf definitions of all configuration and xDS APIs |

## Chapter 8: Distributed System Models and Architectures

Dapr (github.com/dapr/dapr), Go:

| Path | Responsibility |
|------|----------------|
| `cmd/daprd/` | Sidecar entry point |
| `pkg/api/` | HTTP and gRPC APIs exposed to applications |
| `pkg/messaging/` | Service invocation between sidecars |
| `pkg/runtime/` | Component loading, building-block wiring |
| `pkg/actors/` | Virtual actor runtime |
| `pkg/placement/` | Placement service (actor distribution via consistent hashing, Raft for its own state) |
| `pkg/sentry/` | Certificate authority for mTLS |
| `pkg/resiliency/` | Timeouts, retries, circuit breakers |

Components (state stores, pub/sub brokers) live in a separate repository, `dapr/components-contrib`.

## Chapter 9: Reliability, Availability, Scalability, Elasticity and Durability

Kubernetes (github.com/kubernetes/kubernetes):

| Path | Responsibility |
|------|----------------|
| `pkg/controller/podautoscaler/horizontal.go` | Main HPA reconcile loop: fetch metrics, compute desired replicas, apply behavior policies |
| `pkg/controller/podautoscaler/replica_calculator.go` | Replica computation per metric type, handling missing/unready pods |
| `pkg/controller/podautoscaler/metrics/` | Clients for resource, custom, and external metrics APIs |
| `staging/src/k8s.io/api/autoscaling/v2/` | API types |

## Chapter 10: Fault Tolerance and Failure Detection

memberlist (github.com/hashicorp/memberlist):

| File | Responsibility |
|------|----------------|
| `memberlist.go` | Creating/joining a cluster, public API |
| `state.go` | Probe loop, indirect probes, suspicion, alive/dead handling |
| `suspicion.go` | Suspicion timers that accelerate with independent confirmations |
| `awareness.go` | Lifeguard local health awareness |
| `broadcast.go`, `queue.go` | Transmit-limited broadcast queue |
| `net.go`, `net_transport.go` | UDP/TCP transport, message encoding |
| `config.go` | LAN/WAN/local default parameters |

## Chapter 11: The CAP Theorem, Brewer's Theorem and PACELC

Apache Cassandra (github.com/apache/cassandra), Java:

| Path | Responsibility |
|------|----------------|
| `src/java/org/apache/cassandra/db/ConsistencyLevel.java` | Definitions and math for required acknowledgments (blockFor) |
| `src/java/org/apache/cassandra/service/StorageProxy.java` | Coordinator logic for reads/writes, including LWT (Paxos) paths |
| `src/java/org/apache/cassandra/service/reads/` | Read execution, digest reads, read repair |
| `src/java/org/apache/cassandra/service/paxos/` | Paxos implementation for LWT (Paxos v2 in newer versions) |
| `src/java/org/apache/cassandra/hints/` | Hinted handoff |
| `src/java/org/apache/cassandra/gms/` | Gossip and failure detection |

## Chapter 12: Impossibility Results — Two Generals, Byzantine Generals and FLP

CometBFT (github.com/cometbft/cometbft), Go:

| Path | Responsibility |
|------|----------------|
| `consensus/state.go` | The consensus state machine: rounds, steps, timeouts, locking |
| `consensus/reactor.go` | Gossiping proposals and votes with peers |
| `types/vote_set.go` | Collecting votes and detecting 2/3 majorities |
| `types/validator_set.go` | Validator voting power, proposer selection |
| `consensus/wal.go` | Write-ahead log for crash recovery |
| `abci/` | Interface to the application state machine |
| `p2p/` | Peer-to-peer networking |

## Chapter 13: Physical Time and Clock Synchronization

CockroachDB (github.com/cockroachdb/cockroach), Go:

| Path | Responsibility |
|------|----------------|
| `pkg/util/hlc/hlc.go` | HLC implementation: `Now()`, `Update()`, max offset |
| `pkg/util/hlc/timestamp.go` | Timestamp type (WallTime, Logical) |
| `pkg/rpc/clock_offset.go` | Remote clock offset monitoring and enforcement |
| `pkg/kv/kvserver/uncertainty/` | Uncertainty interval computation |
| `pkg/kv/kvclient/kvcoord/` | Transaction coordinator, read refresh/restart logic |

chrony (gitlab.com/chrony/chrony), C: `ntp_core.c` (protocol), `sources.c` (source selection), `reference.c` (clock discipline), `sys_linux.c` (kernel clock adjustment).

- CockroachDB's hybrid logical clock: `pkg/util/hlc` in the CockroachDB repository.

## Chapter 14: Logical Clocks, Vector Clocks and Global State

Apache Flink (github.com/apache/flink), Java:

| Path | Responsibility |
|------|----------------|
| `flink-runtime/.../checkpoint/CheckpointCoordinator.java` | Triggers, tracks, and completes checkpoints |
| `flink-runtime/.../io/checkpointing/` | Barrier handling and alignment (aligned and unaligned) |
| `flink-streaming-java/.../runtime/tasks/StreamTask.java` | Task-level snapshot orchestration |
| `flink-state-backends/flink-statebackend-rocksdb/` | RocksDB state backend, incremental checkpoints |
| `flink-streaming-java/.../sink/TwoPhaseCommitSinkFunction.java` | Two-phase commit sink for exactly-once to external systems |

## Chapter 15: Consistency Models

etcd (github.com/etcd-io/etcd):

| Path | Responsibility |
|------|----------------|
| `server/etcdserver/v3_server.go` | `Range` handling; linearizable read path (`linearizableReadNotify`) |
| `server/etcdserver/server.go` | `linearizableReadLoop` batching ReadIndex requests |
| `go.etcd.io/raft` (`raft.go`, `read_only.go`) | ReadIndex implementation, read-only request tracking |
| `server/storage/mvcc/` | MVCC store with revisions (consistent snapshots) |

## Chapter 16: Distributed Mutual Exclusion and Leader Election

| Repository / path | Responsibility |
|-------------------|----------------|
| `kubernetes/client-go/tools/leaderelection/leaderelection.go` | LeaderElector: acquire, renew, callbacks |
| `kubernetes/client-go/tools/leaderelection/resourcelock/leaselock.go` | Lease-based lock implementation |
| `etcd-io/etcd/client/v3/concurrency/election.go` | Campaign, Proclaim, Resign, Observe |
| `etcd-io/etcd/client/v3/concurrency/mutex.go` | Distributed mutex with waitDeletes on predecessor |
| `apache/curator/curator-recipes/.../leader/LeaderLatch.java` | ZooKeeper leader latch |

- etcd's election and lock helpers: the `client/v3/concurrency` package (module `go.etcd.io/etcd/client/v3/concurrency`), which provides `Mutex`, `Election`, and `Session`.

## Chapter 17: Group Communication, Gossip and Membership

| Path (Apache Cassandra) | Responsibility |
|------------------------|----------------|
| `src/java/org/apache/cassandra/gms/Gossiper.java` | Gossip rounds, peer selection, state merging |
| `src/java/org/apache/cassandra/gms/EndpointState.java` | Per-node state container |
| `src/java/org/apache/cassandra/gms/HeartBeatState.java` | Generation and version |
| `src/java/org/apache/cassandra/gms/FailureDetector.java` | φ accrual implementation |
| `src/java/org/apache/cassandra/gms/GossipDigestSynVerbHandler.java` (and Ack/Ack2) | Message handlers |
| `src/java/org/apache/cassandra/repair/` | Merkle-tree-based repair |

## Chapter 18: Consensus, Paxos and Multi-Paxos

Ceph (github.com/ceph/ceph), C++:

| Path | Responsibility |
|------|----------------|
| `src/mon/Paxos.cc`, `Paxos.h` | Paxos state machine: collect, begin, accept, commit, leases |
| `src/mon/Elector.cc`, `ElectionLogic.cc` | Monitor leader election |
| `src/mon/PaxosService.cc` | Base class for services (OSDMonitor, MonmapMonitor, ...) |
| `src/mon/OSDMonitor.cc` | OSDMap updates proposed through Paxos |
| `src/mon/MonitorDBStore.h` | Key-value storage abstraction |

## Chapter 19: Raft — Understandable Consensus

etcd-io/raft (github.com/etcd-io/raft):

| File | Responsibility |
|------|----------------|
| `raft.go` | Core state machine: `Step`, `stepLeader`, `stepFollower`, `stepCandidate`, `campaign`, `becomeLeader`, commit logic |
| `log.go`, `log_unstable.go` | Raft log: unstable entries, committed/applied tracking |
| `node.go`, `rawnode.go` | Application-facing API; `Ready`/`Advance` |
| `tracker/progress.go`, `tracker/inflights.go` | Per-follower replication progress and flow control |
| `read_only.go` | ReadIndex implementation |
| `confchange/` | Membership change logic (joint consensus) |
| `quorum/` | Majority and joint quorum computations |
| `raftpb/raft.proto` | Message and entry definitions |

In etcd itself: `server/etcdserver/raft.go` (the Ready loop integration), `server/storage/wal/` (WAL), `server/etcdserver/api/rafthttp/` (transport), `server/etcdserver/api/snap/` (snapshots).

- etcd's Raft library is the Go module `go.etcd.io/raft` (formerly the `raft` directory inside the etcd repository).

## Chapter 20: Zab, Viewstamped Replication and Byzantine Fault Tolerance

Apache ZooKeeper (github.com/apache/zookeeper), Java (`zookeeper-server/src/main/java/org/apache/zookeeper/server/`):

| Path | Responsibility |
|------|----------------|
| `quorum/QuorumPeer.java` | Server lifecycle, state transitions |
| `quorum/FastLeaderElection.java` | Leader election |
| `quorum/Leader.java`, `LearnerHandler.java` | Leader logic, per-follower sync and proposal streaming |
| `quorum/Follower.java`, `Learner.java`, `Observer.java` | Follower/observer logic |
| `PrepRequestProcessor.java`, `SyncRequestProcessor.java`, `FinalRequestProcessor.java` | Request pipeline |
| `quorum/CommitProcessor.java` | Commit ordering |
| `persistence/FileTxnLog.java`, `FileSnap.java` | Transaction log and snapshots |
| `DataTree.java` | In-memory znode tree |

## Chapter 21: Coordination Services — ZooKeeper, etcd and Consul

etcd (github.com/etcd-io/etcd):

| Path | Responsibility |
|------|----------------|
| `server/etcdserver/server.go` | EtcdServer: proposal handling, apply loop, linearizable read loop |
| `server/etcdserver/api/v3rpc/` | gRPC service implementations (kv.go, watch.go, lease.go) |
| `server/etcdserver/apply/` | Applying Raft entries to the backend |
| `server/storage/mvcc/kvstore.go`, `watchable_store.go`, `index.go` | MVCC store, watches, B-tree index |
| `server/storage/backend/` | bbolt backend with batched transactions |
| `server/lease/lessor.go` | Lease management |
| `server/storage/wal/` | Write-ahead log |
| `client/v3/concurrency/` | Mutex, Election, STM recipes |

Consul (github.com/hashicorp/consul): `agent/` (agent, health checks, DNS server), `agent/consul/` (server, Raft FSM in `fsm/`, state store in `state/` using go-memdb), and `github.com/hashicorp/raft` for consensus.

## Chapter 22: Transactions — ACID, BASE, Isolation Levels and Concurrency Control

PostgreSQL (`src/backend/`):

| Path | Responsibility |
|------|----------------|
| `access/transam/xact.c` | Transaction lifecycle |
| `access/transam/xlog.c` | WAL management |
| `access/transam/clog.c` | Commit log |
| `storage/ipc/procarray.c` | Snapshots |
| `access/heap/heapam_visibility.c` | Tuple visibility |
| `storage/lmgr/lock.c`, `deadlock.c` | Lock manager, deadlock detection |
| `storage/lmgr/predicate.c` | SSI |
| `commands/vacuum.c`, `access/heap/vacuumlazy.c` | Vacuum |

## Chapter 23: Distributed Transactions — Two-Phase Commit, Three-Phase Commit and Beyond

| Repository / path | Responsibility |
|-------------------|----------------|
| `tikv/tikv/src/storage/txn/actions/prewrite.rs`, `commit.rs`, `check_txn_status.rs` | Percolator actions |
| `tikv/tikv/src/storage/txn/scheduler.rs` | Command scheduling with latches |
| `tikv/tikv/src/storage/mvcc/` | MVCC reader/writer over column families |
| `tikv/client-go/txnkv/transaction/2pc.go` | Client-side two-phase committer used by TiDB |
| `tikv/pd/server/tso/` | Timestamp oracle |
| `cockroachdb/cockroach/pkg/kv/kvclient/kvcoord/txn_coord_sender.go` | CockroachDB transaction coordinator |
| `cockroachdb/cockroach/pkg/kv/kvserver/concurrency/` | Lock table, wait queues, deadlock detection |

## Chapter 24: Sagas and Transaction Coordination in Microservices

Temporal (github.com/temporalio/temporal), Go:

| Path | Responsibility |
|------|----------------|
| `service/frontend/` | Frontend API handlers |
| `service/history/` | History service: shard controller, workflow mutable state, transfer/timer queue processors |
| `service/history/shard/` | Shard ownership and range-ID fencing |
| `service/matching/` | Task queue management and dispatch |
| `common/persistence/` | Persistence interfaces and implementations (Cassandra, SQL) |
| `common/membership/` | Cluster membership and consistent hashing |

*In this table, Cassandra is a distributed database that spreads data over many equal nodes, covered in Chapter 36.*

SDK (e.g., `temporalio/sdk-go/internal/`): workflow replay engine (`internal_event_handlers.go`), activity workers, determinism checks.

## Chapter 25: Replication Fundamentals — Single-Leader, Synchronous, Asynchronous and Semi-Synchronous

Patroni (github.com/patroni/patroni), Python:

| Path | Responsibility |
|------|----------------|
| `patroni/ha.py` | The HA loop: acquire/renew leader key, decide promote/demote |
| `patroni/dcs/` | DCS backends (etcd3.py, consul.py, zookeeper.py, kubernetes.py) |
| `patroni/postgresql/` | PostgreSQL management (bootstrap, replication config, rewind, sync standby) |
| `patroni/api.py` | REST API for health checks and management |
| `patroni/watchdog/` | Watchdog integration |

PostgreSQL (`src/backend/replication/`): `walsender.c`, `walreceiver.c`, `syncrep.c` (synchronous replication waits), `slot.c` (replication slots), `logical/` (logical decoding).

## Chapter 26: Multi-Leader Replication, Leaderless Replication and Quorum Systems

Apache CouchDB (github.com/apache/couchdb), Erlang (a programming language and runtime built for fault-tolerant systems):

| Path | Responsibility |
|------|----------------|
| `src/couch_replicator/` | Replication scheduler, jobs, change feed reader, worker processes, checkpoints |
| `src/couch/src/couch_key_tree.erl` | Revision tree merging |
| `src/couch/src/couch_db.erl` | Document storage and update logic (including `new_edits=false`) |
| `src/chttpd/` | HTTP API endpoints (`_changes`, `_revs_diff`, `_bulk_docs`) |
| `src/mem3/` | Clustering: sharding and internal replication between nodes |

Cassandra hinted handoff and read repair: `org/apache/cassandra/hints/`, `org/apache/cassandra/service/reads/repair/`.

## Chapter 27: Partitioning and Sharding

Vitess (github.com/vitessio/vitess), Go:

| Path | Responsibility |
|------|----------------|
| `go/vt/vtgate/` | Router, executor, query planning (`planbuilder/`), vindexes (`vindexes/`) |
| `go/vt/vttablet/` | Tablet server, query engine, VReplication (`tabletmanager/vreplication/`) |
| `go/vt/topo/` | Topology service abstraction (etcd2topo, zk2topo, consultopo) |
| `go/vt/vtorc/` | Automated failure detection and recovery |
| `go/vt/vtctl/workflow/` | Resharding and MoveTables workflows |
| `go/vt/sqlparser/` | SQL parser |

## Chapter 28: Consistent Hashing

Apache Cassandra (`src/java/org/apache/cassandra/`):

| Path | Responsibility |
|------|----------------|
| `dht/Murmur3Partitioner.java` | Token computation |
| `dht/Range.java`, `dht/Token.java` | Token ranges and arithmetic |
| `locator/TokenMetadata.java` (older) / `tcm/` (newer metadata) | Token ownership |
| `locator/NetworkTopologyStrategy.java` | Rack/DC-aware replica selection |
| `locator/AbstractReplicationStrategy.java` | Replica calculation base |
| `dht/tokenallocator/` | Token allocation algorithm |
| `locator/*Snitch.java` | Topology awareness |

Envoy: `source/extensions/load_balancing_policies/ring_hash/` and `maglev/`.

## Chapter 29: Storage Fundamentals — Block, File and Object Storage

OpenZFS (github.com/openzfs/zfs), C:

| Path | Responsibility |
|------|----------------|
| `module/zfs/dmu*.c` | Data Management Unit, transactions |
| `module/zfs/arc.c` | Adaptive Replacement Cache |
| `module/zfs/zil.c` | ZFS Intent Log |
| `module/zfs/spa*.c` | Storage pool allocator, txg sync |
| `module/zfs/vdev_raidz.c`, `vdev_mirror.c`, `vdev_draid.c` | Redundancy implementations |
| `module/zfs/zio*.c` | I/O pipeline: checksums, compression, encryption |
| `module/os/linux/zfs/zpl_*.c` | Linux POSIX layer |

## Chapter 30: Storage Engines — B-Trees, LSM Trees and Write-Ahead Logs

RocksDB (github.com/facebook/rocksdb), C++:

| Path | Responsibility |
|------|----------------|
| `db/db_impl/db_impl_write.cc` | Write path, write groups, WAL |
| `db/memtable.cc`, `memtable/skiplist.h`, `memtable/inlineskiplist.h` | Memtables |
| `db/flush_job.cc` | Flushing memtables to SSTs |
| `db/compaction/compaction_job.cc`, `compaction_picker_level.cc`, `compaction_picker_universal.cc` | Compaction |
| `db/version_set.cc`, `db/version_edit.cc` | Version management and MANIFEST |
| `table/block_based/` | SST file format, index/filter blocks |
| `table/block_based/filter_policy.cc` | Bloom/Ribbon filters |
| `cache/` | Block cache implementations |
| `utilities/transactions/` | Transaction support |

For comparison, LevelDB (`db/db_impl.cc`, `db/version_set.cc`, `table/`) is a much smaller codebase ideal for first reading.

- LevelDB: repository github.com/google/leveldb; the implementation notes are in `doc/impl.md`.

## Chapter 31: Distributed File Systems — File Service Architecture, NFS and GFS

NFS-Ganesha (github.com/nfs-ganesha/nfs-ganesha), C:

| Path | Responsibility |
|------|----------------|
| `src/MainNFSD/` | Server startup, worker threads, dispatcher |
| `src/Protocols/NFS/` | NFSv3 and NFSv4 operation handlers (e.g., `nfs4_op_open.c`) |
| `src/SAL/` | State management: leases, locks, delegations, recovery |
| `src/FSAL/` | FSAL plugins (FSAL_CEPH, FSAL_GLUSTER, FSAL_VFS, ...) |
| `src/FSAL/Stackable_FSALs/FSAL_MDCACHE/` | Metadata cache |
| `src/RPCAL/` | RPC abstraction, duplicate request cache |

Linux kernel: `fs/nfs/` (client), `fs/nfsd/` (server), `net/sunrpc/` (RPC).

## Chapter 32: HDFS — The Hadoop Distributed File System

Apache Hadoop (github.com/apache/hadoop), `hadoop-hdfs-project/`:

| Path | Responsibility |
|------|----------------|
| `hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java` | Central namespace operations |
| `.../namenode/FSDirectory.java`, `INode*.java` | Inode tree |
| `.../blockmanagement/BlockManager.java`, `BlockPlacementPolicyDefault.java` | Block management and placement |
| `.../namenode/LeaseManager.java` | Leases |
| `.../namenode/FSEditLog.java`, `qjournal/client/QuorumJournalManager.java` | Edit logging and QJM client |
| `.../qjournal/server/Journal.java` | JournalNode storage and epochs |
| `.../server/datanode/DataXceiver.java`, `BlockReceiver.java`, `BlockSender.java` | Data transfer |
| `.../datanode/fsdataset/impl/FsDatasetImpl.java` | Storage volumes |
| `hadoop-hdfs-client/.../DFSOutputStream.java`, `DataStreamer.java`, `DFSInputStream.java` | Client write/read |
| `.../tools/DFSZKFailoverController.java` | ZKFC |
| `hadoop-hdfs-rbf/` | Router-Based Federation |

## Chapter 33: Ceph — Unified Distributed Storage (with a GlusterFS Design Note)

Ceph (github.com/ceph/ceph), C++:

| Path | Responsibility |
|------|----------------|
| `src/crush/` | CRUSH algorithm (`mapper.c`, `CrushWrapper.cc`) |
| `src/mon/` | Monitors, Paxos, OSDMonitor |
| `src/osd/OSD.cc`, `PG.cc`, `PrimaryLogPG.cc`, `PeeringState.cc` | OSD daemon, PG state machine, peering |
| `src/osd/ECBackend.cc`, `ReplicatedBackend.cc` | EC and replicated backends |
| `src/os/bluestore/` | BlueStore, BlueFS |
| `src/msg/async/` | Messenger (msgr2) |
| `src/librados/`, `src/librbd/` | Client libraries |
| `src/rgw/` | RADOS Gateway |
| `src/mds/`, `src/client/` | CephFS MDS and client |
| `src/pybind/mgr/` | Manager modules (balancer, autoscaler, dashboard, orchestrator) |
| `src/crimson/` | Next-generation Seastar-based OSD |

## Chapter 34: Distributed Object Storage — Amazon S3 Concepts, Haystack and the MinIO Design

MinIO (github.com/minio/minio), Go — key files in `cmd/`:

| File | Responsibility |
|------|----------------|
| `api-router.go`, `object-handlers.go`, `bucket-handlers.go` | S3 API routing and handlers |
| `erasure-server-pool.go` | Pools: selecting pools for objects, expansion |
| `erasure-sets.go` | Hashing objects to erasure sets |
| `erasure-object.go`, `erasure-multipart.go` | Object PUT/GET/multipart on an erasure set |
| `erasure-coding.go`, `erasure-encode.go`, `erasure-decode.go` | Reed–Solomon coding |
| `xl-storage.go`, `xl-storage-format-v2.go` | Local drive storage and `xl.meta` format |
| `bitrot*.go` | Bitrot hashing |
| `data-scanner.go`, `erasure-healing.go` | Scanner and healing |
| `internal/dsync/` | Distributed locking |
| `bucket-replication.go`, `site-replication.go` | Replication |

## Chapter 35: Bigtable and HBase — Wide-Column Stores on Distributed File Systems

Apache HBase (github.com/apache/hbase), Java:

| Path | Responsibility |
|------|----------------|
| `hbase-server/.../regionserver/HRegion.java` | Region logic: puts, gets, scans, MVCC, flushes |
| `.../regionserver/HStore.java`, `DefaultMemStore.java`, `CompactingMemStore.java` | Stores and MemStores |
| `.../regionserver/wal/AsyncFSWAL.java` | WAL implementation |
| `.../io/hfile/HFile.java`, `HFileBlock.java`, `HFileReaderImpl.java` | HFile format |
| `.../regionserver/compactions/` | Compaction policies |
| `.../master/assignment/AssignmentManager.java` | Region assignment |
| `.../master/procedure/ServerCrashProcedure.java` | Crash recovery |
| `hbase-procedure/` | Procedure v2 framework |
| `.../wal/WALSplitter.java` | WAL splitting |
| `hbase-client/.../ConnectionImplementation.java` (and async client) | Client, region location caching |

## Chapter 36: Dynamo, DynamoDB and Cassandra — Highly Available Key-Value and Wide-Column Stores

Apache Cassandra (github.com/apache/cassandra), Java (`src/java/org/apache/cassandra/`):

| Path | Responsibility |
|------|----------------|
| `service/StorageProxy.java` | Coordinator read/write logic, CL handling |
| `service/reads/`, `service/reads/repair/` | Read execution, digest reads, read repair |
| `db/ColumnFamilyStore.java`, `db/Keyspace.java` | Table storage management |
| `db/commitlog/` | Commit log |
| `db/memtable/` | Memtable implementations (including TrieMemtable) |
| `io/sstable/` | SSTable formats (BIG, BTI) |
| `db/compaction/` | Compaction strategies (UnifiedCompactionStrategy, etc.) |
| `hints/` | Hinted handoff |
| `repair/` | Repair sessions, Merkle trees |
| `service/paxos/` | LWT Paxos (v1 and v2) |
| `gms/` | Gossip and failure detection |
| `transport/` | CQL native protocol server |
| `index/sai/` | Storage-Attached Indexing |

## Chapter 37: Distributed Databases — SQL, NoSQL, NewSQL and Distributed SQL

MongoDB (github.com/mongodb/mongo), C++ (`src/mongo/`):

| Path | Responsibility |
|------|----------------|
| `db/repl/` | Replication: elections (`topology_coordinator`), oplog fetcher/applier, rollback |
| `db/s/` | Sharding on shards: chunk migration, range deletion, resharding |
| `s/` | mongos routing, query targeting, catalog cache |
| `db/storage/wiredtiger/` | WiredTiger integration |
| `db/transaction/` | Multi-document transactions, 2PC participant/coordinator |
| `db/query/` | Query planning and execution |
| `db/pipeline/` | Aggregation framework |

## Chapter 38: CockroachDB and TiDB — Distributed SQL in Depth

CockroachDB (github.com/cockroachdb/cockroach), Go:

| Path | Responsibility |
|------|----------------|
| `pkg/sql/` | SQL layer (`opt/` optimizer, `colexec/` vectorized engine, `distsql`) |
| `pkg/kv/kvclient/kvcoord/` | TxnCoordSender, interceptors (pipeliner, committer for parallel commits, span refresher), DistSender |
| `pkg/kv/kvserver/` | Stores, replicas, Raft processing, leases, queues (replicate, split, merge, MVCC GC) |
| `pkg/kv/kvserver/concurrency/` | Lock table, wait queues, latch manager |
| `pkg/kv/kvserver/batcheval/` | Evaluation of KV commands (EndTxn, Put, Scan...) |
| `pkg/storage/` | MVCC over Pebble |
| `pkg/util/hlc/` | Hybrid logical clock |
| `github.com/cockroachdb/pebble` | Storage engine |

TiDB/TiKV/PD:

| Repository / path | Responsibility |
|-------------------|----------------|
| `pingcap/tidb/pkg/planner/`, `pkg/executor/` | Optimizer and executors |
| `tikv/client-go/txnkv/transaction/` | Two-phase committer, async commit, 1PC |
| `tikv/tikv/components/raftstore/` | Multi-Raft store |
| `tikv/tikv/src/storage/txn/`, `src/storage/mvcc/` | Percolator transactions and MVCC |
| `tikv/tikv/src/coprocessor/` | Coprocessor pushdown |
| `tikv/pd/server/schedule/`, `pkg/tso/` | Scheduling and TSO |
| `pingcap/tiflash` | Columnar engine and MPP |

## Chapter 39: Messaging Fundamentals and Apache Kafka

Apache Kafka (github.com/apache/kafka), Java and Scala:

| Path | Responsibility |
|------|----------------|
| `core/src/main/scala/kafka/server/KafkaApis.scala` | Request handling |
| `core/src/main/scala/kafka/server/ReplicaManager.scala` | Produce/fetch, ISR |
| `storage/src/main/java/org/apache/kafka/storage/internals/log/` | UnifiedLog, LocalLog, segments, indexes, LogCleaner |
| `metadata/src/main/java/org/apache/kafka/controller/QuorumController.java` | KRaft controller |
| `raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java` | Raft implementation |
| `group-coordinator/` | New group coordinator (KIP-848) |
| `transaction-coordinator/`, `core/.../transaction/` | Transactions |
| `clients/src/main/java/org/apache/kafka/clients/producer/` | Producer (RecordAccumulator, Sender) |
| `clients/src/main/java/org/apache/kafka/clients/consumer/` | Consumer |
| `streams/` | Kafka Streams |
| `connect/` | Kafka Connect |

## Chapter 40: RabbitMQ, Apache Pulsar and NATS

| Repository / path | Responsibility |
|-------------------|----------------|
| `apache/pulsar/pulsar-broker/.../service/persistent/PersistentTopic.java` | Topic logic |
| `apache/pulsar/managed-ledger/.../ManagedLedgerImpl.java`, `ManagedCursorImpl.java` | Ledger chain and cursors |
| `apache/pulsar/pulsar-broker/.../service/persistent/PersistentDispatcher*.java` | Subscription dispatchers |
| `apache/bookkeeper/bookkeeper-server/.../bookie/` | Bookie journal and storage |
| `apache/bookkeeper/bookkeeper-server/.../client/LedgerHandle.java` | Ledger client, fencing/recovery |
| `rabbitmq/rabbitmq-server/deps/rabbit/src/rabbit_quorum_queue.erl`, `rabbitmq/ra` | Quorum queues and Raft |
| `rabbitmq/rabbitmq-server/deps/rabbit/src/rabbit_exchange_type_topic.erl` | Topic routing |
| `nats-io/nats-server/server/jetstream*.go`, `raft.go`, `stream.go`, `consumer.go`, `filestore.go` | JetStream and Raft |
| `nats-io/nats-server/server/sublist.go` | Subject matching |

## Chapter 41: MapReduce and Hadoop — Batch Processing at Scale

Apache Hadoop (github.com/apache/hadoop):

| Path | Responsibility |
|------|----------------|
| `hadoop-mapreduce-project/.../mapred/MapTask.java` | Map execution, MapOutputBuffer (sort and spill) |
| `.../mapred/ReduceTask.java`, `.../task/reduce/Shuffle.java`, `Fetcher.java`, `MergeManagerImpl.java` | Reduce-side shuffle and merge |
| `.../mapreduce/v2/app/MRAppMaster.java` | ApplicationMaster |
| `.../mapreduce/v2/app/speculate/` | Speculative execution |
| `.../mapred/ShuffleHandler.java` | NodeManager auxiliary shuffle service |
| `hadoop-yarn-project/.../resourcemanager/` | RM, schedulers (`scheduler/capacity/`, `scheduler/fair/`) |
| `hadoop-yarn-project/.../nodemanager/` | NM, container management |

## Chapter 42: Apache Spark and Apache Flink — In-Memory Batch and Stream Processing

| Repository / path | Responsibility |
|-------------------|----------------|
| `apache/spark/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala` | Stage creation and scheduling |
| `.../scheduler/TaskSchedulerImpl.scala`, `TaskSetManager.scala` | Task scheduling, locality, speculation |
| `.../storage/BlockManager.scala` | Block storage |
| `.../shuffle/sort/SortShuffleManager.scala` | Shuffle |
| `sql/catalyst/` | Catalyst optimizer |
| `sql/core/.../execution/adaptive/` | AQE |
| `sql/core/.../execution/streaming/` | Structured Streaming |
| `apache/flink/flink-runtime/.../checkpoint/CheckpointCoordinator.java` | Checkpoints |
| `.../streaming/runtime/tasks/StreamTask.java` | Task execution |
| `.../io/network/` | Network stack, credit-based flow control |
| `flink-state-backends/flink-statebackend-rocksdb/` | RocksDB backend |
| `flink-table/` | Table API / SQL planner |

## Chapter 43: Virtualization, Containers and Docker

| Repository / path | Responsibility |
|-------------------|----------------|
| `containerd/containerd/core/content/`, `core/snapshots/`, `core/metadata/` | Content store, snapshots, metadata |
| `containerd/containerd/internal/cri/` (or `pkg/cri/`) | CRI implementation for Kubernetes |
| `containerd/containerd/core/runtime/v2/` | Shim management |
| `opencontainers/runc/libcontainer/` | Namespaces, cgroups, init process |
| `moby/moby/daemon/` | Docker Engine daemon |
| `moby/buildkit/solver/` | Build graph solver and cache |
| `firecracker-microvm/firecracker/src/vmm/` | Firecracker VMM |

## Chapter 44: Kubernetes and etcd — Orchestrating Distributed Systems

Kubernetes (github.com/kubernetes/kubernetes), Go:

| Path | Responsibility |
|------|----------------|
| `staging/src/k8s.io/apiserver/` | Generic API server machinery (handlers, admission, storage, watch cache) |
| `pkg/controlplane/` | kube-apiserver assembly |
| `pkg/scheduler/` | Scheduler and framework plugins |
| `pkg/controller/` | Built-in controllers (deployment, replicaset, statefulset, nodelifecycle, endpointslice) |
| `pkg/kubelet/` | Kubelet (pod workers, PLEG, probes, volume manager, eviction) |
| `pkg/proxy/` | kube-proxy modes |
| `staging/src/k8s.io/client-go/tools/cache/` | Informers, reflectors, work queues |
| `sigs.k8s.io/controller-runtime` | Operator framework |
| `etcd-io/etcd` | etcd server (Chapter 21) |

## Chapter 45: Service Mesh and Microservice Resilience Patterns

| Repository / path | Responsibility |
|-------------------|----------------|
| `envoyproxy/envoy/source/server/` | Server, hot restart, workers |
| `envoyproxy/envoy/source/common/http/conn_manager_impl.cc` | HTTP connection manager |
| `envoyproxy/envoy/source/common/router/` | Routing, retries, timeouts |
| `envoyproxy/envoy/source/common/upstream/` | Clusters, load balancers, outlier detection, health checks |
| `envoyproxy/envoy/source/extensions/filters/` | Filters (RBAC, JWT, rate limit, ext_authz, WASM) |
| `envoyproxy/envoy/source/common/config/` | xDS subscription handling |
| `istio/istio/pilot/pkg/` | istiod discovery and xDS generation |
| `istio/istio/security/pkg/` | CA and certificate management |
| `istio/ztunnel` (Rust) | Ambient node proxy |
| `linkerd/linkerd2-proxy` (Rust) | Linkerd data plane |

## Chapter 46: Observability — Monitoring, Metrics, Logging, Distributed Tracing and Alerting

| Repository / path | Responsibility |
|-------------------|----------------|
| `prometheus/prometheus/scrape/` | Scrape manager and loops |
| `prometheus/prometheus/discovery/` | Service discovery mechanisms |
| `prometheus/prometheus/tsdb/` | Head, WAL, blocks, index, compaction |
| `prometheus/prometheus/tsdb/chunkenc/` | Gorilla-style chunk encoding, native histograms |
| `prometheus/prometheus/promql/` | Query engine and parser |
| `prometheus/prometheus/rules/` | Recording and alerting rules |
| `prometheus/alertmanager/` | Alert routing, grouping, inhibition, silences (gossip via memberlist) |
| `open-telemetry/opentelemetry-collector` and `-contrib` | Collector core and processors (tail sampling) |

## Chapter 47: Site Reliability Engineering, Incident Management and Disaster Recovery

| Repository / path | Responsibility |
|-------------------|----------------|
| `chaos-mesh/chaos-mesh/controllers/` | Chaos experiment controllers |
| `chaos-mesh/chaos-mesh/pkg/chaosdaemon/` | Node-level fault injection |
| `vmware-tanzu/velero/pkg/backup/`, `pkg/restore/` | Backup and restore logic |
| `vmware-tanzu/velero/pkg/plugin/` | Plugin framework |
| `prometheus/alertmanager/dispatch/`, `notify/` | Alert routing and notification |
| `grafana/k6` | Load testing engine |

## Chapter 48: Distributed Systems Security — Authentication, Authorization, TLS, Certificates and Secrets

| Repository / path | Responsibility |
|-------------------|----------------|
| `openbao/openbao/vault/` (or `hashicorp/vault/vault/`) | Core, router, barrier, expiration manager, policy store |
| `.../builtin/logical/` | Secrets engines (database, pki, transit) |
| `.../builtin/credential/` | Auth methods |
| `.../physical/raft/` | Integrated Raft storage |
| `spiffe/spire/pkg/server/`, `pkg/agent/` | SPIRE server (CA, registration) and agent (workload API, attestors) |
| `open-policy-agent/opa/` | Rego evaluation (`topdown/`), bundles, decision logs |
| `authzed/spicedb/` | Zanzibar-style authorization |
| `cert-manager/cert-manager/` | Kubernetes certificate automation (ACME, Vault, CA issuers) |

## Chapter 49: Event Sourcing and CQRS

| Repository / path | Responsibility |
|-------------------|----------------|
| `kurrent-io/KurrentDB/src/EventStore.Core/Services/Storage/` | Storage writer, reader, chunk management |
| `.../EventStore.Core/Index/` | Stream index (PTables) |
| `.../EventStore.Core/Services/Replication/` | Leader/follower replication |
| `.../EventStore.Core/Services/PersistentSubscription/` | Persistent subscriptions |
| `.../EventStore.Projections.Core/` | Server-side projections |
| `AxonFramework/AxonFramework/eventsourcing/` | Axon event sourcing repositories, snapshotters |
| `JasperFx/marten/src/Marten/Events/` | Marten event store on PostgreSQL |

## Chapter 50: CRDTs — Conflict-Free Replicated Data Types

| Repository / path | Responsibility |
|-------------------|----------------|
| `yjs/yjs/src/structs/Item.js` | Item structure and YATA integration |
| `yjs/yjs/src/utils/Transaction.js` | Transactions and events |
| `yjs/yjs/src/utils/encoding.js`, `updates.js` | Update encoding and merging |
| `yjs/yjs/src/types/YText.js`, `YArray.js`, `YMap.js` | Shared types |
| `automerge/automerge/rust/automerge/src/` | Automerge core (op set, columnar storage, sync protocol) |

## Chapter 51: Distributed Caching

| Repository / path | Responsibility |
|-------------------|----------------|
| `valkey-io/valkey/src/server.c` (or `redis/redis/src/server.c`) | Main loop, command table, serverCron |
| `src/ae.c` | Event loop |
| `src/networking.c` | Client I/O, RESP parsing |
| `src/dict.c` | Hash table with incremental rehashing |
| `src/t_string.c`, `t_hash.c`, `t_zset.c`, `t_stream.c` | Data types |
| `src/expire.c`, `src/evict.c` | Expiration and eviction (approximate LRU/LFU) |
| `src/rdb.c`, `src/aof.c` | Persistence |
| `src/replication.c` | Replication |
| `src/cluster.c` / `cluster_legacy.c` | Cluster bus and slot management |
| `memcached/memcached/` (`slabs.c`, `items.c`, `thread.c`) | Memcached internals |

## Chapter 52: Geo-Replication, Multi-Region Architectures and Edge Computing

| Repository / path | Responsibility |
|-------------------|----------------|
| `cockroachdb/cockroach/pkg/sql/multiregion/` | Multi-region abstractions to zone configs |
| `cockroachdb/cockroach/pkg/kv/kvserver/allocator/` | Region-aware replica and lease placement |
| `cockroachdb/cockroach/pkg/kv/kvserver/closedts/` | Closed timestamps (follower reads) |
| `apache/kafka/connect/mirror/` | MirrorMaker 2 connectors |
| `superfly/litefs` | Edge SQLite replication via FUSE |
| `coredns/coredns/plugin/` | DNS plugins (geo-aware routing via plugins/external) |

## Chapter 53: Industry Case Studies — Google, Amazon, Microsoft, Netflix, Uber, LinkedIn and Meta

| Repository | Where to start |
|------------|----------------|
| `kubernetes/kubernetes` | `pkg/scheduler/`, `pkg/controller/` |
| `firecracker-microvm/firecracker` | `src/vmm/` |
| `dotnet/orleans` | `src/Orleans.Runtime/` (catalog, directory, messaging) |
| `uber/cadence` | `service/history/`, `service/matching/` |
| `apache/kafka` | `core/.../ReplicaManager.scala`, `metadata/.../QuorumController.java` |
| `facebook/rocksdb` | `db/db_impl/`, `db/compaction/` |
| `facebook/mcrouter` | `mcrouter/` routing handles |
| `uber/h3` | `src/h3lib/lib/` |

## Chapter 54: The System Design Interview — Framework, Estimation and Trade-Off Reasoning

Not applicable in the usual sense. Instead, practice by reading the source code entry points listed in earlier chapters (e.g., Kafka's `ReplicaManager`, etcd's `raft` package, Redis's `server.c`) to speak credibly about internals during deep dives.

## Chapter 55: System Design Problems — Worked Solutions

| Repository | Relevant code |
|------------|---------------|
| `envoyproxy/ratelimit` | Global rate limit service (Redis backend) |
| `uber/h3` | Geospatial indexing |
| `temporalio/temporal` | `service/worker/scheduler/` (schedules) |
| `FFmpeg/FFmpeg` | Transcoding |
| `debezium/debezium` | Outbox event router |

## Chapter 56: Capstone Project — Building a Replicated, Sharded Key-Value Store

| Repository / path | What to study |
|-------------------|---------------|
| `etcd-io/raft/raft.go` | Role transitions, message handling (`stepLeader`, `stepFollower`) |
| `etcd-io/raft/log.go`, `log_unstable.go` | Log management |
| `etcd-io/raft/read_only.go` | ReadIndex implementation |
| `etcd-io/raft/tracker/` | Per-follower progress, flow control |
| `etcd-io/etcd/server/etcdserver/raft.go` | How etcd drives the raft library (Ready loop) |
| `hashicorp/raft/raft.go` | Alternative Raft design with integrated goroutines |
| `tikv/raft-rs` and `tikv/tikv/components/raftstore/` | Multi-Raft at scale |
| `anishathalye/porcupine` | Linearizability checking algorithm |

- etcd's Raft library: Go module `go.etcd.io/raft/v3`.
