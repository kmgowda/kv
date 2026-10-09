package kv;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import kv.check.Linearizability;
import kv.store.Command;
import kv.store.KvStateMachine;
import kv.store.Result;
import kv.raft.Entry;
import kv.raft.FileStorage;
import kv.raft.MemoryStorage;
import kv.raft.Message;
import kv.raft.RaftNode;
import kv.raft.Storage;
import kv.sim.Cluster;
import kv.sim.SimClient;

/**
 * The kv test suite. Run with: {@code ./gradlew test} (add {@code -Pseeds=1000} for more chaos seeds).
 * Plain Java (no test framework) so it runs anywhere a JDK 21 is installed.
 */
public final class TestMain {
    private static int passed, failed;

    public static void main(String[] args) throws Exception {
        int chaosSeeds = args.length > 0 ? Integer.parseInt(args[0]) : 50;
        run("same-term step-down keeps the vote (no double voting)", TestMain::sameTermStepDownKeepsVote);
        run("AppendEntries inside the snapshot is rejected", TestMain::appendInsideSnapshotRejected);
        run("earlier-term entries never commit by counting (Figure 8)", TestMain::noCommitOfOldTermByCounting);
        run("follower commits only the prefix the leader verified", TestMain::commitOnlyVerifiedPrefix);
        run("a vote survives a crash (persist before reply)", TestMain::voteSurvivesCrash);
        run("one-node group commits and serves reads", TestMain::singleNodeCommits);
        run("session table: duplicates and stale sequences", TestMain::sessionSemantics);
        run("leader in a minority partition cannot commit", TestMain::minorityLeaderCannotCommit);
        run("lagging follower catches up via InstallSnapshot", TestMain::snapshotCatchUp);
        run("file storage: reopen, torn tail, interrupted snapshot", TestMain::fileStorageRecovery);
        run("lossy, duplicating network: history is linearizable", TestMain::lossyNetworkLinearizable);
        run("randomized chaos (" + chaosSeeds + " seeds): crashes, partitions, loss",
                () -> chaos(chaosSeeds));
        System.out.printf("%n%d passed, %d failed%n", passed, failed);
        System.exit(failed == 0 ? 0 : 1);
    }

    // ---------------------------------------------------------------------------------------------
    // Unit tests on a single RaftNode
    // ---------------------------------------------------------------------------------------------

    /** Captures what a RaftNode sends and applies, so tests can drive it by hand. */
    static final class Recorder implements RaftNode.Env {
        final List<Message> sent = new ArrayList<>();
        final List<Entry> applied = new ArrayList<>();

        @Override public void send(int to, Message m) { sent.add(m); }
        @Override public void apply(Entry e) { applied.add(e); }
        @Override public void restoreSnapshot(long index, long term, byte[] data) {}
        @Override public void readReady(long requestId) {}
        @Override public void lostLeadership() {}

        <T extends Message> T last(Class<T> type) {
            for (int i = sent.size() - 1; i >= 0; i--) if (type.isInstance(sent.get(i))) return type.cast(sent.get(i));
            return null;
        }
    }

    /**
     * Regression test for a classic Raft bug: a candidate that steps down because a
     * leader of the SAME term exists must keep its vote, or it could vote twice in that term.
     */
    static void sameTermStepDownKeepsVote() {
        Recorder env = new Recorder();
        MemoryStorage storage = new MemoryStorage();
        RaftNode a = new RaftNode(0, new int[] {1, 2}, storage, env, 1, 5, 5, 1);
        for (int i = 0; i < 5; i++) a.tick();                    // election timeout: candidate in term 1
        check(a.role() == RaftNode.Role.CANDIDATE && a.term() == 1 && a.votedFor() == 0, "candidate in term 1");

        a.step(1, new Message.AppendEntries(1, 1, 0, 0, List.of(), 0, 0));   // node 1 won term 1
        check(a.role() == RaftNode.Role.FOLLOWER, "steps down to follower");
        check(a.votedFor() == 0 && storage.hardState().votedFor() == 0, "vote for itself is kept");

        a.step(2, new Message.RequestVote(1, 2, 0, 0));          // node 2 also asks for a vote in term 1
        Message.RequestVoteReply reply = env.last(Message.RequestVoteReply.class);
        check(reply != null && !reply.granted(), "second vote in the same term is refused");
    }

    /** A follower whose log is compacted up to 8 must not accept an AppendEntries anchored at 5. */
    static void appendInsideSnapshotRejected() {
        Recorder env = new Recorder();
        RaftNode f = new RaftNode(0, new int[] {1, 2}, new MemoryStorage(), env, 1, 10, 20, 1);
        List<Entry> entries = new ArrayList<>();
        for (int i = 1; i <= 10; i++) entries.add(new Entry(1, i, new byte[] {(byte) i}));
        f.step(1, new Message.AppendEntries(1, 1, 0, 0, entries, 10, 0));
        check(f.lastApplied() == 10, "applied 10 entries");
        f.compact(8, new byte[] {42});
        check(f.snapshotIndex() == 8 && f.logLength() == 2, "log compacted to snapshot 8 plus 2 entries");

        f.step(1, new Message.AppendEntries(1, 1, 5, 1,
                List.of(new Entry(1, 6, new byte[] {6}), new Entry(1, 7, new byte[] {7})), 10, 0));
        Message.AppendEntriesReply r = env.last(Message.AppendEntriesReply.class);
        check(!r.success() && r.conflictIndex() == 9, "rejected with a hint to continue at index 9");
        check(f.snapshotIndex() == 8 && f.logLength() == 2, "log unchanged");
    }

    /**
     * The Raft paper's Figure 8 hazard: a new leader must not mark an entry from an EARLIER term
     * committed just because a majority stores it; only entries of its own term commit by counting.
     */
    static void noCommitOfOldTermByCounting() {
        MemoryStorage storage = new MemoryStorage();
        storage.saveHardState(3, -1);
        storage.append(List.of(new Entry(1, 1, new byte[] {1}), new Entry(2, 2, new byte[] {2})));
        Recorder env = new Recorder();
        RaftNode leader = new RaftNode(0, new int[] {1, 2}, storage, env, 1, 5, 5, 1);
        for (int i = 0; i < 5; i++) leader.tick();               // candidate in term 4
        leader.step(1, new Message.RequestVoteReply(4, true));
        check(leader.role() == RaftNode.Role.LEADER && leader.lastIndex() == 3, "leader of term 4 with its no-op at 3");

        leader.step(1, new Message.AppendEntriesReply(4, true, 2, 0, -1, 0));   // peer 1 holds 1..2
        check(leader.commitIndex() == 0, "term-2 entry stored on a majority is NOT committed by counting");

        leader.step(1, new Message.AppendEntriesReply(4, true, 3, 0, -1, 0));   // peer 1 holds the no-op
        check(leader.commitIndex() == 3 && env.applied.size() == 3, "committing the term-4 no-op commits 1..3");
    }

    /**
     * A follower holds a stale, uncommitted entry 3 from an old leader. A heartbeat from the new
     * leader verifies only entries up to 2, so even with leaderCommit = 3 the follower must not
     * commit its own (possibly different) entry 3.
     */
    static void commitOnlyVerifiedPrefix() {
        MemoryStorage storage = new MemoryStorage();
        storage.saveHardState(1, -1);
        storage.append(List.of(new Entry(1, 1, new byte[] {1}), new Entry(1, 2, new byte[] {2}),
                new Entry(1, 3, new byte[] {99})));                // 3 = stale entry from the old leader
        Recorder env = new Recorder();
        RaftNode f = new RaftNode(0, new int[] {1, 2}, storage, env, 1, 10, 20, 1);
        f.step(1, new Message.AppendEntries(2, 1, 2, 1, List.of(), 3, 0));
        check(f.commitIndex() == 2, "committed up to 2, not the unverified entry 3");
    }

    /** A node votes, crashes, restarts from storage, and is asked again in the same term. */
    static void voteSurvivesCrash() {
        MemoryStorage disk = new MemoryStorage();
        Recorder env = new Recorder();
        RaftNode before = new RaftNode(0, new int[] {1, 2}, disk, env, 1, 10, 20, 1);
        before.step(1, new Message.RequestVote(5, 1, 0, 0));
        check(env.last(Message.RequestVoteReply.class).granted(), "first vote in term 5 granted");

        Recorder env2 = new Recorder();
        RaftNode after = new RaftNode(0, new int[] {1, 2}, disk, env2, 2, 10, 20, 1);   // restart
        after.step(2, new Message.RequestVote(5, 2, 0, 0));
        check(!env2.last(Message.RequestVoteReply.class).granted(), "no second vote in term 5 after restart");
    }

    static void sessionSemantics() {
        KvStateMachine sm = new KvStateMachine();
        Command first = new Command(7, 1, Command.Op.APPEND, "k", "a", null);
        sm.apply(first);
        sm.apply(first);                                          // retry of the same request
        check("a".equals(sm.read("k").value()), "duplicate APPEND executed once");
        sm.apply(new Command(7, 2, Command.Op.APPEND, "k", "b", null));
        Result stale = sm.apply(new Command(7, 1, Command.Op.APPEND, "k", "z", null));
        check(stale.status() == Result.Status.STALE_SEQUENCE, "older sequence number is rejected");
        check("ab".equals(sm.read("k").value()), "state unaffected by the stale request");

        KvStateMachine copy = new KvStateMachine();
        copy.restore(sm.snapshot());                              // sessions travel inside snapshots
        copy.apply(new Command(7, 2, Command.Op.APPEND, "k", "b", null));
        check("ab".equals(copy.read("k").value()), "deduplication survives snapshot/restore");
    }

    // ---------------------------------------------------------------------------------------------
    // Cluster tests in the simulator
    // ---------------------------------------------------------------------------------------------

    static void singleNodeCommits() {
        Cluster c = new Cluster(1, 3, 1000);
        SimClient client = c.addClient(4, 30, new String[] {"a", "b"});
        check(c.runUntilClientsIdle(5000), "all operations completed");
        check(client.history().size() == 30, "30 operations recorded");
        assertHealthy(c);
    }

    static void minorityLeaderCannotCommit() {
        Cluster c = new Cluster(5, 11, 1000);
        c.run(200);
        int old = c.leader();
        check(old >= 0, "a leader was elected");
        int buddy = (old + 1) % 5;
        c.partition(Set.of(old, buddy));                          // old leader + 1 follower: a minority
        long frozen = c.server(old).raft().commitIndex();
        c.server(old).raft().propose(new Command(1, 1, Command.Op.PUT, "x", "lost?", null).encode());
        c.run(300);
        check(c.server(old).raft().commitIndex() == frozen, "minority leader committed nothing");
        int fresh = c.leader();
        check(fresh != old && c.server(fresh).raft().term() > c.server(old).raft().term(),
                "majority side elected a newer leader");
        c.heal();
        c.run(300);
        check(c.server(old).raft().role() != RaftNode.Role.LEADER || c.server(old).raft().term() > 1,
                "old leader stepped down after healing");
        long agreed = c.server(c.leader()).raft().commitIndex();
        for (int i = 0; i < 5; i++) check(c.server(i).raft().commitIndex() == agreed, "all servers caught up");
        assertHealthy(c);
    }

    static void snapshotCatchUp() {
        Cluster c = new Cluster(3, 21, 20);                       // compact after 20 entries
        c.run(100);
        c.isolate(2);
        for (int i = 0; i < 4; i++) c.addClient(100 + i, 60, new String[] {"a", "b", "c"});
        check(c.runUntilClientsIdle(20000), "clients finished while server 2 was cut off");
        c.heal();
        c.run(500);
        check(c.server(2).raft().snapshotIndex() > 0, "server 2 received a snapshot");
        Map<String, String> expected = c.server(c.leader()).stateMachine().contents();
        check(c.server(2).stateMachine().contents().equals(expected), "server 2 has the same data");
        assertHealthy(c);
    }

    static void lossyNetworkLinearizable() {
        Cluster c = new Cluster(3, 31, 50);
        c.dropRate = 0.2;
        c.duplicateRate = 0.1;
        c.maxDelay = 6;
        for (int i = 0; i < 5; i++) c.addClient(200 + i, 80, new String[] {"a", "b"});
        check(c.runUntilClientsIdle(60000), "all operations completed despite loss");
        assertHealthy(c);
    }

    /** Random faults for many seeds. Every check must hold for every seed. */
    static void chaos(int seeds) {
        for (int seed = 1; seed <= seeds; seed++) {
            Random r = new Random(seed * 7919L);
            int n = r.nextBoolean() ? 3 : 5;
            Cluster c = new Cluster(n, seed, 15 + r.nextInt(40));
            c.dropRate = r.nextDouble() * 0.15;
            c.duplicateRate = r.nextDouble() * 0.1;
            c.maxDelay = 2 + r.nextInt(6);
            for (int i = 0; i < 4; i++) c.addClient(seed * 100L + i, 60, new String[] {"a", "b", "c"});
            for (int t = 0; t < 4000; t++) {
                int fault = r.nextInt(400);
                if (fault == 0) c.crash(r.nextInt(n));
                else if (fault == 1) c.restart(r.nextInt(n));
                else if (fault == 2) c.isolate(r.nextInt(n));
                else if (fault == 3) {
                    Set<Integer> side = new java.util.HashSet<>();
                    for (int i = 0; i < n; i++) if (r.nextBoolean()) side.add(i);
                    c.partition(side);
                } else if (fault == 4) c.heal();
                c.tick();
            }
            c.heal();                                             // let the system recover, then finish
            for (int i = 0; i < n; i++) c.restart(i);
            c.dropRate = 0;
            check(c.runUntilClientsIdle(40000), "seed " + seed + ": all operations completed");
            c.run(300);
            assertHealthy(c);
            Map<String, String> expected = c.server(c.leader()).stateMachine().contents();
            for (int i = 0; i < n; i++) {
                check(c.server(i).stateMachine().contents().equals(expected), "seed " + seed + ": replicas converged");
            }
        }
    }

    /** Safety invariants plus linearizability of everything the clients observed. */
    static void assertHealthy(Cluster c) {
        check(c.violations().isEmpty(), "Raft invariants hold " + c.violations());
        List<Linearizability.Op> history = new ArrayList<>();
        for (SimClient client : c.clients()) history.addAll(client.history());
        for (Linearizability.Op op : history) {
            check(op.result().status() != Result.Status.STALE_SEQUENCE, "no stale sequence numbers");
        }
        String problem = Linearizability.check(history);
        check(problem == null, "client history is linearizable: " + problem);
    }

    // ---------------------------------------------------------------------------------------------
    // Storage
    // ---------------------------------------------------------------------------------------------

    static void fileStorageRecovery() throws IOException {
        Path dir = Files.createTempDirectory("kv-test");
        FileStorage s = new FileStorage(dir);
        s.saveHardState(3, 1);
        List<Entry> es = new ArrayList<>();
        for (int i = 1; i <= 5; i++) es.add(entry(1, i, "v" + i));
        s.append(es);
        s.truncateFrom(4);                                        // conflict: replace 4 and 5
        s.append(List.of(entry(2, 4, "w4"), entry(2, 5, "w5")));
        s.close();

        FileStorage reopened = new FileStorage(dir);
        check(reopened.hardState().equals(new Storage.HardState(3, 1)), "hard state survives restart");
        check(reopened.entries().size() == 5 && reopened.entries().get(3).term() == 2, "log survives restart");
        reopened.close();

        // Torn tail: chop the last 3 bytes off the WAL, as if power failed mid-write.
        Path wal = dir.resolve("wal");
        try (FileChannel ch = FileChannel.open(wal, StandardOpenOption.WRITE)) {
            ch.truncate(ch.size() - 3);
        }
        FileStorage torn = new FileStorage(dir);
        check(torn.entries().size() == 4, "torn final record discarded, earlier records kept");

        // Interrupted snapshot install: new snapshot written, but the WAL is still the old one.
        Path oldWal = dir.resolve("wal.copy");
        Files.copy(wal, oldWal, StandardCopyOption.REPLACE_EXISTING);
        torn.installSnapshot(new Storage.Snapshot(3, 1, "state@3".getBytes(StandardCharsets.UTF_8)),
                List.of(entry(2, 4, "w4")));
        torn.close();
        Files.copy(oldWal, wal, StandardCopyOption.REPLACE_EXISTING);   // simulate crash between the steps
        FileStorage after = new FileStorage(dir);
        check(after.snapshot().index() == 3, "snapshot survives");
        check(after.entries().size() == 1 && after.entries().get(0).index() == 4,
                "stale WAL from the previous generation is ignored");
        after.close();
    }

    private static Entry entry(long term, long index, String cmd) {
        return new Entry(term, index, cmd.getBytes(StandardCharsets.UTF_8));
    }

    // ---------------------------------------------------------------------------------------------
    // Tiny test runner
    // ---------------------------------------------------------------------------------------------

    interface TestBody { void run() throws Exception; }

    private static void run(String name, TestBody body) {
        long start = System.nanoTime();
        try {
            body.run();
            passed++;
            System.out.printf("PASS  %-62s %6d ms%n", name, (System.nanoTime() - start) / 1_000_000);
        } catch (Throwable t) {
            failed++;
            System.out.printf("FAIL  %s%n      %s%n", name, t);
        }
    }

    private static void check(boolean condition, String what) {
        if (!condition) throw new AssertionError(what);
    }
}
