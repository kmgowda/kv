package kv.store;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * The replicated state machine: a key-value map plus a client session table.
 *
 * <p>Deterministic by construction: no clocks, randomness, or I/O, and sorted maps so snapshots of
 * equal states are byte-for-byte equal.
 *
 * <h2>Retry deduplication and its assumptions</h2>
 * The session table stores, per client, the highest sequence number executed and its result.
 * This is correct ONLY under the client discipline enforced by {@link kv.sim.SimClient}:
 * <ol>
 *   <li>each client has at most one outstanding write at a time;</li>
 *   <li>it retries with the same sequence number until it receives a final answer;</li>
 *   <li>it then moves to a strictly higher sequence number.</li>
 * </ol>
 * Under these rules a retry can only carry the latest sequence number, so returning the cached
 * result is correct. A request with an OLDER sequence number violates rule 1 and is rejected with
 * {@code STALE_SEQUENCE} instead of being answered with another operation's result.
 * Clients that pipeline several writes need a per-sequence result window instead.
 * Sessions are never expired here; production systems expire them deterministically
 * (for example using leader-assigned timestamps that are themselves written to the log).
 */
public final class KvStateMachine {
    private record Session(long lastSeq, Result lastResult) {}

    private final TreeMap<String, String> data = new TreeMap<>();
    private final TreeMap<Long, Session> sessions = new TreeMap<>();

    /** Apply one committed write. Returns the result for the client. */
    public Result apply(Command c) {
        Session s = sessions.get(c.clientId());
        if (s != null && c.seq() == s.lastSeq()) return s.lastResult();          // duplicate: no re-execution
        if (s != null && c.seq() < s.lastSeq()) return Result.of(Result.Status.STALE_SEQUENCE);
        Result r = execute(c);
        sessions.put(c.clientId(), new Session(c.seq(), r));
        return r;
    }

    /** Local read; the caller must have completed the ReadIndex protocol first. */
    public Result read(String key) {
        String v = data.get(key);
        return v == null ? Result.of(Result.Status.NOT_FOUND) : Result.ok(v);
    }

    private Result execute(Command c) {
        return switch (c.op()) {
            case PUT -> { data.put(c.key(), c.value()); yield Result.ok(null); }
            case APPEND -> { data.merge(c.key(), c.value(), String::concat); yield Result.ok(null); }
            case DELETE -> { data.remove(c.key()); yield Result.ok(null); }
            case CAS -> {
                if (Objects.equals(data.get(c.key()), c.expected())) {
                    data.put(c.key(), c.value());
                    yield Result.ok(null);
                }
                yield Result.of(Result.Status.CAS_MISMATCH);
            }
            case GET -> throw new IllegalArgumentException("GET is never written to the log");
        };
    }

    // ---------------- snapshots: data AND sessions, so deduplication survives restarts ----------------

    public byte[] snapshot() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeInt(data.size());
            for (Map.Entry<String, String> e : data.entrySet()) {
                out.writeUTF(e.getKey());
                out.writeUTF(e.getValue());
            }
            out.writeInt(sessions.size());
            for (Map.Entry<Long, Session> e : sessions.entrySet()) {
                out.writeLong(e.getKey());
                out.writeLong(e.getValue().lastSeq());
                out.writeByte(e.getValue().lastResult().status().ordinal());
                Command.writeNullable(out, e.getValue().lastResult().value());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bos.toByteArray();
    }

    public void restore(byte[] snapshot) {
        data.clear();
        sessions.clear();
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(snapshot))) {
            int n = in.readInt();
            for (int i = 0; i < n; i++) data.put(in.readUTF(), in.readUTF());
            int m = in.readInt();
            for (int i = 0; i < m; i++) {
                long client = in.readLong(), seq = in.readLong();
                Result r = new Result(Result.Status.values()[in.readByte()], Command.readNullable(in));
                sessions.put(client, new Session(seq, r));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Map<String, String> contents() { return Map.copyOf(data); }
}
