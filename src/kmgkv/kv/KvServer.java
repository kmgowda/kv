package kmgkv.kv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import kmgkv.raft.Entry;
import kmgkv.raft.Message;
import kmgkv.raft.RaftNode;
import kmgkv.raft.Storage;

/**
 * One kmgkv server: a RaftNode plus the KV state machine, glued together.
 *
 * <ul>
 *   <li>Writes are proposed to Raft and answered when THEIR entry is applied. The reply is sent
 *       only if the entry applied at that index has the term we proposed in; otherwise another
 *       leader's entry took the slot and the client is told to retry (deduplication makes the
 *       retry safe).</li>
 *   <li>Reads use ReadIndex: no log write, but a leadership confirmation round.</li>
 *   <li>When the log grows beyond {@code maxLogEntries}, the server snapshots the state machine
 *       (data + sessions) and lets Raft discard the log prefix.</li>
 * </ul>
 */
public final class KvServer implements RaftNode.Env {

    /** How the server talks to the outside world (the simulator, or a real RPC layer). */
    public interface Transport {
        void sendRaft(int to, Message m);

        void reply(int client, long requestId, Result result);
    }

    /** Test hook: observes every applied entry and installed snapshot. */
    public interface Observer {
        void applied(int server, Entry e);

        void snapshotInstalled(int server, long index);
    }

    private record PendingWrite(long term, int client, long requestId) {}

    private record PendingRead(int client, long requestId, String key) {}

    private final int id;
    private final Transport transport;
    private final Observer observer;
    private final int maxLogEntries;
    private final KvStateMachine sm = new KvStateMachine();
    private final Map<Long, PendingWrite> pendingWrites = new HashMap<>();   // by log index
    private final Map<Long, PendingRead> pendingReads = new HashMap<>();     // by local read id
    private long nextReadId;
    private final RaftNode raft;

    public KvServer(int id, int[] peers, Storage storage, Transport transport, Observer observer,
                    long seed, int maxLogEntries) {
        this.id = id;
        this.transport = transport;
        this.observer = observer;
        this.maxLogEntries = maxLogEntries;
        this.raft = new RaftNode(id, peers, storage, this, seed, 10, 20, 3);
        raft.start();                                             // restores the saved snapshot, if any
    }

    // ---------------- inputs from the host ----------------

    public void tick() {
        raft.tick();
        if (raft.logLength() > maxLogEntries) {
            raft.compact(raft.lastApplied(), sm.snapshot());     // sm reflects exactly lastApplied
        }
    }

    public void onRaftMessage(int from, Message m) {
        raft.step(from, m);
    }

    public void onClientRequest(int client, long requestId, Command c) {
        if (raft.role() != RaftNode.Role.LEADER) {
            transport.reply(client, requestId, Result.of(Result.Status.WRONG_LEADER));
            return;
        }
        if (c.op() == Command.Op.GET) {
            long rid = ++nextReadId;
            pendingReads.put(rid, new PendingRead(client, requestId, c.key()));
            raft.requestRead(rid);                                // may complete synchronously
            return;
        }
        // Register before proposing: in a one-node group the entry commits and applies inside propose().
        long index = raft.lastIndex() + 1;
        pendingWrites.put(index, new PendingWrite(raft.term(), client, requestId));
        Entry e = raft.propose(c.encode());
        if (e == null || e.index() != index) throw new IllegalStateException("leader failed to append");
    }

    // ---------------- RaftNode.Env ----------------

    @Override public void send(int to, Message m) {
        transport.sendRaft(to, m);
    }

    @Override public void apply(Entry e) {
        if (observer != null) observer.applied(id, e);
        Result r = e.isNoop() ? null : sm.apply(Command.decode(e.command()));
        PendingWrite p = pendingWrites.remove(e.index());
        if (p != null) {
            // Defensive: losing leadership already fails all pending writes, so a different-term
            // entry should never meet a pending write here. If it ever does, it is not our command.
            boolean ours = p.term() == e.term() && r != null;
            transport.reply(p.client(), p.requestId(), ours ? r : Result.of(Result.Status.WRONG_LEADER));
        }
    }

    @Override public void restoreSnapshot(long index, long term, byte[] data) {
        sm.restore(data);
        if (observer != null) observer.snapshotInstalled(id, index);
        failPending(index);
    }

    @Override public void readReady(long readId) {
        PendingRead p = pendingReads.remove(readId);
        if (p != null) transport.reply(p.client(), p.requestId(), sm.read(p.key()));
    }

    @Override public void lostLeadership() {
        failPending(Long.MAX_VALUE);
        for (PendingRead p : pendingReads.values()) {
            transport.reply(p.client(), p.requestId(), Result.of(Result.Status.WRONG_LEADER));
        }
        pendingReads.clear();
    }

    /** Tell clients waiting on log indexes <= upTo to retry. Their retry is deduplicated if needed. */
    private void failPending(long upTo) {
        for (Long index : new ArrayList<>(pendingWrites.keySet())) {
            if (index <= upTo) {
                PendingWrite p = pendingWrites.remove(index);
                transport.reply(p.client(), p.requestId(), Result.of(Result.Status.WRONG_LEADER));
            }
        }
    }

    // ---------------- inspection (tests, metrics) ----------------

    public RaftNode raft() { return raft; }

    public KvStateMachine stateMachine() { return sm; }
}
