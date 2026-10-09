package kv.sim;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import kv.check.Linearizability.Op;
import kv.store.Command;
import kv.store.Result;

/**
 * A simulated client that follows the retry discipline the state machine relies on:
 * one outstanding operation at a time; retry with the SAME sequence number on timeout or
 * WRONG_LEADER; use a new sequence number only after a final answer.
 * Every completed operation is recorded (invocation time, response time, result) for the
 * linearizability checker.
 */
public final class SimClient {
    private static final int TIMEOUT_TICKS = 25;

    private final Cluster cluster;
    private final int address;
    private final long clientId;
    private final Random rnd;
    private final int servers;
    private final int maxOps;
    private final String[] keys;
    private final List<Op> history = new ArrayList<>();

    private int issued;
    private long seq;
    private int target;
    private Command current;
    private long currentRequestId;
    private long requestCounter;
    private long invokedAt;
    private long sentAt;
    private boolean stopped;

    SimClient(Cluster cluster, int address, Random rnd, int servers, int maxOps, String[] keys) {
        this.cluster = cluster;
        this.address = address;
        this.rnd = rnd;
        this.clientId = rnd.nextLong();          // random 64-bit ID: a restarted client would pick a new one
        this.servers = servers;
        this.maxOps = maxOps;
        this.keys = keys;
    }

    void tick(long now) {
        if (current == null) {
            if (!stopped && issued < maxOps && rnd.nextInt(3) == 0) issue(now);
        } else if (now - sentAt > TIMEOUT_TICKS) {
            target = (target + 1) % servers;     // no answer: try another server, same seq
            send(now);
        }
    }

    void onReply(long requestId, Result r, long now) {
        if (current == null || requestId != currentRequestId) return;   // late reply to an old attempt
        if (!r.isFinal()) {
            target = (target + 1) % servers;
            send(now);
            return;
        }
        history.add(new Op(address, current.op(), current.key(), current.value(), current.expected(),
                invokedAt, now, r));
        current = null;
    }

    /** Stop issuing new operations (the current one, if any, still completes). */
    public void stop() { stopped = true; }

    public boolean idle() { return current == null && (stopped || issued >= maxOps); }

    public List<Op> history() { return history; }

    private void issue(long now) {
        issued++;
        String key = keys[rnd.nextInt(keys.length)];
        String v = Integer.toString(address) + "." + issued;     // unique values make duplicates visible
        current = switch (rnd.nextInt(5)) {
            case 0 -> Command.get(clientId, 0, key);
            case 1 -> new Command(clientId, ++seq, Command.Op.PUT, key, v, null);
            case 2 -> new Command(clientId, ++seq, Command.Op.APPEND, key, "[" + v + "]", null);
            case 3 -> new Command(clientId, ++seq, Command.Op.CAS, key, v, rnd.nextBoolean() ? null : "x");
            default -> Command.get(clientId, 0, key);
        };
        invokedAt = now;
        send(now);
    }

    private void send(long now) {
        sentAt = now;
        currentRequestId = ++requestCounter;
        cluster.sendFromClient(address, target, currentRequestId, current);
    }
}
