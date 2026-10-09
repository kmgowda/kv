package kmgkv.sim;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;

import kmgkv.kv.Command;
import kmgkv.kv.KvServer;
import kmgkv.kv.Result;
import kmgkv.raft.Entry;
import kmgkv.raft.MemoryStorage;
import kmgkv.raft.Message;
import kmgkv.raft.RaftNode;

/**
 * A deterministic, single-threaded cluster simulator.
 *
 * <p>Time is a tick counter. The network delivers each message after a random delay and can drop,
 * duplicate, and (through random delays) reorder messages, and block links to model partitions.
 * Servers can crash (all volatile state is lost; {@link MemoryStorage} survives, like a disk) and
 * restart. Every random choice comes from one seeded {@link Random}, so a failing seed replays
 * the exact same execution.
 *
 * <p>While running, the simulator checks two Raft safety properties continuously:
 * <ul>
 *   <li><b>Election safety:</b> at most one leader per term.</li>
 *   <li><b>State machine safety:</b> no two servers apply different entries at the same index.</li>
 * </ul>
 */
public final class Cluster {
    public static final int CLIENT_BASE = 1000;

    sealed interface Payload {}

    record Raft(Message m) implements Payload {}

    record ClientRequest(long requestId, Command command) implements Payload {}

    record ClientReply(long requestId, Result result) implements Payload {}

    private record Delivery(long time, long order, int from, int to, Payload payload) {}

    private final Random rnd;
    private final int n;
    private final int maxLogEntries;
    private final MemoryStorage[] storages;
    private final KvServer[] servers;
    private final boolean[] up;
    private final PriorityQueue<Delivery> queue = new PriorityQueue<>(
            (a, b) -> a.time != b.time ? Long.compare(a.time, b.time) : Long.compare(a.order, b.order));
    private final Set<Long> blocked = new HashSet<>();
    private final List<SimClient> clients = new ArrayList<>();
    private final Map<Long, Integer> leaderOfTerm = new HashMap<>();
    private final Map<Long, Entry> appliedAt = new HashMap<>();
    private final List<String> violations = new ArrayList<>();
    private long now;
    private long order;

    // Network behavior (tests change these).
    public double dropRate = 0.0;
    public double duplicateRate = 0.0;
    public int minDelay = 1;
    public int maxDelay = 3;

    public Cluster(int n, long seed, int maxLogEntries) {
        this.rnd = new Random(seed);
        this.n = n;
        this.maxLogEntries = maxLogEntries;
        this.storages = new MemoryStorage[n];
        this.servers = new KvServer[n];
        this.up = new boolean[n];
        for (int i = 0; i < n; i++) {
            storages[i] = new MemoryStorage();
            start(i);
        }
    }

    // ---------------- running ----------------

    public void tick() {
        now++;
        while (!queue.isEmpty() && queue.peek().time() <= now) deliver(queue.poll());
        for (int i = 0; i < n; i++) if (up[i]) servers[i].tick();
        for (SimClient c : clients) c.tick(now);
        checkElectionSafety();
    }

    public void run(int ticks) {
        for (int i = 0; i < ticks; i++) tick();
    }

    /** Runs until every client is idle or {@code maxTicks} pass. Returns true if all are idle. */
    public boolean runUntilClientsIdle(int maxTicks) {
        for (int i = 0; i < maxTicks; i++) {
            if (clients.stream().allMatch(SimClient::idle)) return true;
            tick();
        }
        return clients.stream().allMatch(SimClient::idle);
    }

    // ---------------- faults ----------------

    public void crash(int i) {
        up[i] = false;
        servers[i] = null;                                        // all volatile state is gone
    }

    public void restart(int i) {
        if (!up[i]) start(i);
    }

    /** Servers in {@code side} can talk to each other but not to the rest (clients reach everyone). */
    public void partition(Set<Integer> side) {
        blocked.clear();
        for (int a = 0; a < n; a++)
            for (int b = 0; b < n; b++)
                if (side.contains(a) != side.contains(b)) blocked.add(link(a, b));
    }

    public void isolate(int i) {
        partition(Set.of(i));
    }

    public void heal() {
        blocked.clear();
    }

    // ---------------- clients ----------------

    public SimClient addClient(long seed, int maxOps, String[] keys) {
        SimClient c = new SimClient(this, CLIENT_BASE + clients.size(), new Random(seed), n, maxOps, keys);
        clients.add(c);
        return c;
    }

    void sendFromClient(int client, int server, long requestId, Command c) {
        send(client, server, new ClientRequest(requestId, c));
    }

    // ---------------- inspection ----------------

    public KvServer server(int i) { return servers[i]; }

    public boolean isUp(int i) { return up[i]; }

    public int size() { return n; }

    public long now() { return now; }

    public List<SimClient> clients() { return clients; }

    public List<String> violations() { return violations; }

    /** The current leader with the highest term among running servers, or -1. */
    public int leader() {
        int best = -1;
        for (int i = 0; i < n; i++) {
            if (up[i] && servers[i].raft().role() == RaftNode.Role.LEADER
                    && (best < 0 || servers[i].raft().term() > servers[best].raft().term())) best = i;
        }
        return best;
    }

    // ---------------- internals ----------------

    private void start(int i) {
        int[] peers = new int[n - 1];
        for (int j = 0, k = 0; j < n; j++) if (j != i) peers[k++] = j;
        KvServer.Transport t = new KvServer.Transport() {
            @Override public void sendRaft(int to, Message m) { send(i, to, new Raft(m)); }

            @Override public void reply(int client, long requestId, Result r) {
                send(i, client, new ClientReply(requestId, r));
            }
        };
        KvServer.Observer obs = new KvServer.Observer() {
            @Override public void applied(int server, Entry e) { checkStateMachineSafety(server, e); }

            @Override public void snapshotInstalled(int server, long index) {}
        };
        up[i] = true;
        servers[i] = new KvServer(i, peers, storages[i], t, obs, rnd.nextLong(), maxLogEntries);
    }

    private void send(int from, int to, Payload p) {
        if (blocked.contains(link(from, to)) || rnd.nextDouble() < dropRate) return;
        enqueue(from, to, p);
        if (rnd.nextDouble() < duplicateRate) enqueue(from, to, p);
    }

    private void enqueue(int from, int to, Payload p) {
        long delay = minDelay + rnd.nextInt(maxDelay - minDelay + 1);
        queue.add(new Delivery(now + delay, order++, from, to, p));
    }

    private void deliver(Delivery d) {
        if (blocked.contains(link(d.from(), d.to()))) return;    // partitioned while in flight
        if (d.to() >= CLIENT_BASE) {
            if (d.payload() instanceof ClientReply r) clients.get(d.to() - CLIENT_BASE).onReply(r.requestId(), r.result(), now);
            return;
        }
        if (!up[d.to()]) return;                                  // destination is down
        switch (d.payload()) {
            case Raft r -> servers[d.to()].onRaftMessage(d.from(), r.m());
            case ClientRequest cr -> servers[d.to()].onClientRequest(d.from(), cr.requestId(), cr.command());
            case ClientReply ignored -> { }
        }
    }

    private void checkElectionSafety() {
        for (int i = 0; i < n; i++) {
            if (!up[i] || servers[i].raft().role() != RaftNode.Role.LEADER) continue;
            long term = servers[i].raft().term();
            Integer prev = leaderOfTerm.putIfAbsent(term, i);
            if (prev != null && prev != i) violations.add("two leaders in term " + term + ": " + prev + " and " + i);
        }
    }

    private void checkStateMachineSafety(int server, Entry e) {
        Entry first = appliedAt.putIfAbsent(e.index(), e);
        if (first != null && (first.term() != e.term() || !Arrays.equals(first.command(), e.command()))) {
            violations.add("server " + server + " applied a different entry at index " + e.index());
        }
    }

    private static long link(int a, int b) {
        return ((long) a << 32) | (b & 0xffffffffL);
    }
}
