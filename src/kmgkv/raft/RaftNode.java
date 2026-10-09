package kmgkv.raft;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import kmgkv.raft.Message.AppendEntries;
import kmgkv.raft.Message.AppendEntriesReply;
import kmgkv.raft.Message.InstallSnapshot;
import kmgkv.raft.Message.InstallSnapshotReply;
import kmgkv.raft.Message.RequestVote;
import kmgkv.raft.Message.RequestVoteReply;

/**
 * A deterministic, single-threaded Raft core in the style of etcd's raft library.
 *
 * <p>The node has no threads, timers, sockets, or clocks of its own. The host drives it by calling
 * {@link #tick()} (one unit of logical time), {@link #step(int, Message)} (a message arrived),
 * {@link #propose(byte[])}, {@link #requestRead(long)}, and {@link #compact(long, byte[])}.
 * Effects leave through {@link Env}. Because nothing happens "on its own", a test can replay the
 * exact same execution from a random seed.
 *
 * <p>Every state change that Raft's safety depends on is written to {@link Storage} before any
 * message that depends on it is passed to {@link Env#send}.
 *
 * <p>Not implemented (see Chapter 56, "Scope of the tested code"): membership changes,
 * pre-vote/check-quorum, leader leases, chunked snapshots, pipelining with flow control.
 */
public final class RaftNode {
    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    /** Everything the node asks its host to do. All calls happen inside tick/step/propose/etc. */
    public interface Env {
        void send(int to, Message m);

        /** A committed entry, delivered exactly once per node lifetime and strictly in index order. */
        void apply(Entry e);

        /** Replace the state machine with a snapshot (on restart, or when the leader sent one). */
        void restoreSnapshot(long index, long term, byte[] data);

        /** A ReadIndex read may now be served from the local state machine. */
        void readReady(long requestId);

        /** This node stopped being leader: pending reads and writes should be failed or retried. */
        void lostLeadership();
    }

    private record PendingRead(long requestId, long ctx, long readIndex) {}

    private static final int MAX_BATCH = 64;

    private final int id;
    private final int[] peers;          // other members of the group (never includes id)
    private final Storage storage;
    private final Env env;
    private final Random random;
    private final int electionTicksMin, electionTicksMax, heartbeatTicks;

    // Persistent state (mirrored in storage). log.get(0) is a sentinel holding the snapshot's index/term.
    private long currentTerm;
    private int votedFor;
    private final List<Entry> log = new ArrayList<>();

    // Volatile state.
    private Role role = Role.FOLLOWER;
    private int leaderId = -1;
    private long commitIndex;
    private long lastApplied;
    private int electionElapsed;
    private int electionTimeout;
    private int heartbeatElapsed;
    private final Set<Integer> votes = new HashSet<>();

    // Leader-only state.
    private final Map<Integer, Long> nextIndex = new HashMap<>();
    private final Map<Integer, Long> matchIndex = new HashMap<>();
    private final Map<Integer, Long> ackedCtx = new HashMap<>();
    private long noopIndex;             // index of the no-op this leader appended when elected
    private long readCtx;               // increases with every new read request
    private final List<PendingRead> pendingReads = new ArrayList<>();

    public RaftNode(int id, int[] peers, Storage storage, Env env, long seed,
                    int electionTicksMin, int electionTicksMax, int heartbeatTicks) {
        this.id = id;
        this.peers = peers.clone();
        this.storage = storage;
        this.env = env;
        this.random = new Random(seed);
        this.electionTicksMin = electionTicksMin;
        this.electionTicksMax = electionTicksMax;
        this.heartbeatTicks = heartbeatTicks;

        Storage.HardState hs = storage.hardState();
        currentTerm = hs.term();
        votedFor = hs.votedFor();
        Storage.Snapshot snap = storage.snapshot();
        log.add(new Entry(snap.term(), snap.index(), null));      // sentinel
        log.addAll(storage.entries());
        commitIndex = snap.index();                               // a snapshot only ever contains committed entries
        lastApplied = snap.index();
        resetElectionTimer();
    }

    /** Must be called once after construction: hands any saved snapshot to the state machine. */
    public void start() {
        Storage.Snapshot snap = storage.snapshot();
        if (snap.index() > 0) env.restoreSnapshot(snap.index(), snap.term(), snap.data());
    }

    // =====================================================================================
    // Public API
    // =====================================================================================

    /** Advance logical time by one tick. */
    public void tick() {
        if (role == Role.LEADER) {
            if (++heartbeatElapsed >= heartbeatTicks) broadcastAppend();
        } else if (++electionElapsed >= electionTimeout) {
            startElection();
        }
    }

    /** Handle a message from {@code from}. */
    public void step(int from, Message m) {
        if (m.term() > currentTerm) {
            // Rule for all servers: a higher term always wins. This is the ONLY place where
            // votedFor is cleared, because a vote is valid for exactly one term.
            adoptHigherTerm(m.term());
        }
        switch (m) {
            case RequestVote rv -> handleRequestVote(from, rv);
            case RequestVoteReply r -> handleVoteReply(from, r);
            case AppendEntries ae -> handleAppendEntries(from, ae);
            case AppendEntriesReply r -> handleAppendReply(from, r);
            case InstallSnapshot is -> handleInstallSnapshot(from, is);
            case InstallSnapshotReply r -> handleSnapshotReply(from, r);
        }
    }

    /**
     * Append a command if this node is leader. Returns the new entry (its index and term let the
     * caller recognize the result when it is applied), or null if this node is not the leader.
     * The entry may still be lost if leadership changes before it commits.
     */
    public Entry propose(byte[] command) {
        if (role != Role.LEADER) return null;
        Entry e = appendLocal(command);
        advanceCommit();                                          // a one-node group commits immediately
        broadcastAppend();
        return e;
    }

    /**
     * Start a linearizable read (ReadIndex). Returns false if this node is not the leader.
     * Later, {@link Env#readReady} fires once (1) a majority has confirmed this node is still leader
     * after the read arrived and (2) the state machine has applied at least the read index.
     */
    public boolean requestRead(long requestId) {
        if (role != Role.LEADER) return false;
        long ctx = ++readCtx;
        // A new leader does not know the latest commit index until its own no-op commits.
        long readIndex = commitIndex >= noopIndex ? commitIndex : -1;
        pendingReads.add(new PendingRead(requestId, ctx, readIndex));
        if (peers.length == 0) checkReads(); else broadcastAppend();
        return true;
    }

    /**
     * The state machine has captured its state as of {@code index} (which it has applied).
     * Persist the snapshot, then drop the log prefix it covers.
     */
    public void compact(long index, byte[] data) {
        if (index <= snapshotIndex() || index > lastApplied) return;
        Storage.Snapshot s = new Storage.Snapshot(index, termAt(index), data);
        List<Entry> retained = new ArrayList<>(log.subList(offset(index) + 1, log.size()));
        storage.installSnapshot(s, retained);                     // durable first ...
        log.clear();                                              // ... then forget the prefix
        log.add(new Entry(s.term(), s.index(), null));
        log.addAll(retained);
    }

    public Role role() { return role; }
    public long term() { return currentTerm; }
    public int leaderHint() { return leaderId; }
    public long commitIndex() { return commitIndex; }
    public long lastApplied() { return lastApplied; }
    public long lastIndex() { return lastEntry().index(); }
    public int logLength() { return log.size() - 1; }
    public long snapshotIndex() { return log.get(0).index(); }
    public int votedFor() { return votedFor; }

    // =====================================================================================
    // Elections
    // =====================================================================================

    private void startElection() {
        currentTerm++;
        votedFor = id;
        persistHardState();                                       // remember our own vote before asking for others
        role = Role.CANDIDATE;
        leaderId = -1;
        votes.clear();
        votes.add(id);
        resetElectionTimer();
        if (votes.size() >= majority()) {                         // one-node group
            becomeLeader();
            return;
        }
        Entry last = lastEntry();
        RequestVote rv = new RequestVote(currentTerm, id, last.index(), last.term());
        for (int p : peers) env.send(p, rv);
    }

    private void handleRequestVote(int from, RequestVote rv) {
        Entry last = lastEntry();
        // Election restriction: only vote for a candidate whose log is at least as up to date.
        boolean upToDate = rv.lastLogTerm() > last.term()
                || (rv.lastLogTerm() == last.term() && rv.lastLogIndex() >= last.index());
        boolean grant = rv.term() == currentTerm
                && (votedFor == -1 || votedFor == rv.candidate())  // at most one vote per term
                && upToDate;
        if (grant) {
            votedFor = rv.candidate();
            persistHardState();                                   // the vote must survive a crash
            resetElectionTimer();
        }
        env.send(from, new RequestVoteReply(currentTerm, grant));
    }

    private void handleVoteReply(int from, RequestVoteReply r) {
        if (role != Role.CANDIDATE || r.term() != currentTerm || !r.granted()) return;
        votes.add(from);
        if (votes.size() >= majority()) becomeLeader();
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = id;
        nextIndex.clear();
        matchIndex.clear();
        ackedCtx.clear();
        pendingReads.clear();
        for (int p : peers) {
            nextIndex.put(p, lastEntry().index() + 1);
            matchIndex.put(p, 0L);
            ackedCtx.put(p, 0L);
        }
        noopIndex = appendLocal(null).index();                    // commit something in our own term
        advanceCommit();
        broadcastAppend();
    }

    /** A message with a higher term arrived: adopt it, clear the vote, become follower. */
    private void adoptHigherTerm(long term) {
        boolean wasLeader = role == Role.LEADER;
        currentTerm = term;
        votedFor = -1;
        persistHardState();
        role = Role.FOLLOWER;
        leaderId = -1;
        if (wasLeader) loseLeadership();
    }

    /**
     * Same-term step-down: a candidate learns that another node already won this term.
     * The term and our vote stay as they are; clearing votedFor here would allow a second
     * vote in the same term and break election safety.
     */
    private void followLeaderOfCurrentTerm(int leader) {
        if (role == Role.LEADER) {
            throw new IllegalStateException("two leaders in term " + currentTerm); // impossible in Raft
        }
        role = Role.FOLLOWER;
        leaderId = leader;
        resetElectionTimer();
    }

    private void loseLeadership() {
        pendingReads.clear();
        env.lostLeadership();
    }

    // =====================================================================================
    // Log replication
    // =====================================================================================

    private void broadcastAppend() {
        heartbeatElapsed = 0;
        for (int p : peers) sendAppend(p);
    }

    private void sendAppend(int to) {
        long next = nextIndex.get(to);
        if (next <= snapshotIndex()) {                            // the entries it needs are compacted
            Storage.Snapshot s = storage.snapshot();
            env.send(to, new InstallSnapshot(currentTerm, id, s.index(), s.term(), s.data(), readCtx));
            return;
        }
        long prev = next - 1;
        long last = Math.min(lastEntry().index(), prev + MAX_BATCH);
        List<Entry> batch = List.copyOf(log.subList(offset(prev) + 1, offset(last) + 1));
        env.send(to, new AppendEntries(currentTerm, id, prev, termAt(prev), batch, commitIndex, readCtx));
    }

    private void handleAppendEntries(int from, AppendEntries ae) {
        if (ae.term() < currentTerm) {                            // from a deposed leader
            env.send(from, new AppendEntriesReply(currentTerm, false, 0, 0, -1, ae.ctx()));
            return;
        }
        followLeaderOfCurrentTerm(from);

        // 1. prevLogIndex falls inside our snapshot. Those entries are committed, so we cannot
        //    compare terms entry by entry. Reject, and tell the leader to continue right after
        //    our snapshot, where the check is possible again.
        if (ae.prevLogIndex() < snapshotIndex()) {
            env.send(from, new AppendEntriesReply(currentTerm, false, 0, snapshotIndex() + 1, -1, ae.ctx()));
            return;
        }
        // 2. Our log is too short.
        if (ae.prevLogIndex() > lastEntry().index()) {
            env.send(from, new AppendEntriesReply(currentTerm, false, 0, lastEntry().index() + 1, -1, ae.ctx()));
            return;
        }
        // 3. Log Matching check: we must hold the leader's entry at prevLogIndex.
        long localTerm = termAt(ae.prevLogIndex());
        if (localTerm != ae.prevLogTerm()) {
            long first = ae.prevLogIndex();
            while (first - 1 > snapshotIndex() && termAt(first - 1) == localTerm) first--;
            env.send(from, new AppendEntriesReply(currentTerm, false, 0, first, localTerm, ae.ctx()));
            return;
        }
        // 4. Append, truncating only at the first real conflict. Entries we already hold are
        //    skipped, so a delayed, shorter AppendEntries can never erase newer entries.
        List<Entry> toAppend = new ArrayList<>();
        for (Entry e : ae.entries()) {
            if (!toAppend.isEmpty()) {
                toAppend.add(e);
            } else if (e.index() > lastEntry().index()) {
                toAppend.add(e);
            } else if (termAt(e.index()) != e.term()) {
                if (e.index() <= commitIndex) {
                    throw new IllegalStateException("leader tried to overwrite a committed entry");
                }
                storage.truncateFrom(e.index());
                log.subList(offset(e.index()), log.size()).clear();
                toAppend.add(e);
            }
        }
        if (!toAppend.isEmpty()) {
            storage.append(toAppend);                             // durable before we acknowledge
            log.addAll(toAppend);
        }
        long lastNew = ae.prevLogIndex() + ae.entries().size();
        if (ae.leaderCommit() > commitIndex) {
            commitIndex = Math.min(ae.leaderCommit(), lastNew);   // never beyond what this message verified
            applyCommitted();
        }
        env.send(from, new AppendEntriesReply(currentTerm, true, lastNew, 0, -1, ae.ctx()));
    }

    private void handleAppendReply(int from, AppendEntriesReply r) {
        if (role != Role.LEADER || r.term() != currentTerm) return;   // stale reply
        ackedCtx.merge(from, r.ctx(), Math::max);
        if (r.success()) {
            if (r.matchIndex() > matchIndex.get(from)) matchIndex.put(from, r.matchIndex());
            nextIndex.put(from, Math.max(nextIndex.get(from), matchIndex.get(from) + 1));
            advanceCommit();
            if (nextIndex.get(from) <= lastEntry().index()) sendAppend(from);   // keep catching up
        } else {
            long next = r.conflictIndex();
            if (r.conflictTerm() >= 0) {
                long lastOfTerm = lastIndexOfTerm(r.conflictTerm());
                if (lastOfTerm > 0) next = lastOfTerm + 1;
            }
            next = Math.max(next, matchIndex.get(from) + 1);      // replies can arrive out of order
            next = Math.min(next, lastEntry().index() + 1);
            nextIndex.put(from, Math.max(1, next));
            sendAppend(from);
        }
        checkReads();
    }

    private void handleInstallSnapshot(int from, InstallSnapshot is) {
        if (is.term() < currentTerm) {
            env.send(from, new InstallSnapshotReply(currentTerm, 0, is.ctx()));
            return;
        }
        followLeaderOfCurrentTerm(from);
        long idx = is.lastIncludedIndex();
        if (idx > commitIndex) {
            List<Entry> retained = new ArrayList<>();
            if (idx <= lastEntry().index() && idx >= snapshotIndex() && termAt(idx) == is.lastIncludedTerm()) {
                retained.addAll(log.subList(offset(idx) + 1, log.size()));   // our suffix still matches
            }
            Storage.Snapshot s = new Storage.Snapshot(idx, is.lastIncludedTerm(), is.data());
            storage.installSnapshot(s, retained);
            log.clear();
            log.add(new Entry(s.term(), s.index(), null));
            log.addAll(retained);
            commitIndex = idx;
            lastApplied = idx;
            env.restoreSnapshot(idx, s.term(), s.data());
        }
        // Either way, everything up to idx is now committed here, so it matches the leader.
        env.send(from, new InstallSnapshotReply(currentTerm, idx, is.ctx()));
    }

    private void handleSnapshotReply(int from, InstallSnapshotReply r) {
        if (role != Role.LEADER || r.term() != currentTerm) return;
        ackedCtx.merge(from, r.ctx(), Math::max);
        if (r.matchIndex() > matchIndex.get(from)) matchIndex.put(from, r.matchIndex());
        nextIndex.put(from, Math.max(nextIndex.get(from), matchIndex.get(from) + 1));
        advanceCommit();
        if (nextIndex.get(from) <= lastEntry().index()) sendAppend(from);
        checkReads();
    }

    /**
     * Commit the highest index stored on a majority, but only by counting replicas of an entry
     * from the CURRENT term; earlier-term entries then commit with it (Raft paper, Figure 8).
     */
    private void advanceCommit() {
        if (role != Role.LEADER) return;
        for (long n = lastEntry().index(); n > commitIndex; n--) {
            if (termAt(n) != currentTerm) break;                  // earlier terms: never by counting
            int count = 1;                                        // ourselves
            for (int p : peers) if (matchIndex.get(p) >= n) count++;
            if (count >= majority()) {
                commitIndex = n;
                applyCommitted();
                break;
            }
        }
        checkReads();
    }

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            env.apply(entryAt(lastApplied));
        }
        checkReads();
    }

    // =====================================================================================
    // ReadIndex
    // =====================================================================================

    private void checkReads() {
        if (role != Role.LEADER || pendingReads.isEmpty()) return;
        List<Long> ready = new ArrayList<>();
        for (int i = 0; i < pendingReads.size(); i++) {
            PendingRead r = pendingReads.get(i);
            if (r.readIndex() < 0 && commitIndex >= noopIndex) {  // our no-op committed: fix the read index
                r = new PendingRead(r.requestId(), r.ctx(), commitIndex);
                pendingReads.set(i, r);
            }
            int confirmations = 1;
            for (int p : peers) if (ackedCtx.get(p) >= r.ctx()) confirmations++;
            if (r.readIndex() >= 0 && confirmations >= majority() && lastApplied >= r.readIndex()) {
                ready.add(r.requestId());
            }
        }
        if (ready.isEmpty()) return;
        for (Iterator<PendingRead> it = pendingReads.iterator(); it.hasNext(); ) {
            if (ready.contains(it.next().requestId())) it.remove();
        }
        for (long reqId : ready) env.readReady(reqId);
    }

    // =====================================================================================
    // Helpers
    // =====================================================================================

    private Entry appendLocal(byte[] command) {
        Entry e = new Entry(currentTerm, lastEntry().index() + 1, command);
        storage.append(List.of(e));                               // durable before anyone hears of it
        log.add(e);
        return e;
    }

    private void persistHardState() { storage.saveHardState(currentTerm, votedFor); }

    private int majority() { return (peers.length + 1) / 2 + 1; }

    private void resetElectionTimer() {
        electionElapsed = 0;
        electionTimeout = electionTicksMin + random.nextInt(electionTicksMax - electionTicksMin + 1);
    }

    private Entry lastEntry() { return log.get(log.size() - 1); }

    private int offset(long index) { return (int) (index - snapshotIndex()); }

    private Entry entryAt(long index) { return log.get(offset(index)); }

    private long termAt(long index) { return entryAt(index).term(); }

    private long lastIndexOfTerm(long term) {
        for (int i = log.size() - 1; i >= 1; i--) {
            long t = log.get(i).term();
            if (t == term) return log.get(i).index();
            if (t < term) break;
        }
        return 0;
    }
}
