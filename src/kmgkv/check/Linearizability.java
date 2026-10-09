package kmgkv.check;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import kmgkv.kv.Command;
import kmgkv.kv.Result;

/**
 * A linearizability checker for single-key register-like histories (the Wing-Gong algorithm with
 * Lowe's memoization, the same idea used by Porcupine and Knossos).
 *
 * <p>A history is linearizable if we can pick, for every operation, one instant between its
 * invocation and its response, such that executing the operations one by one in that order on a
 * single copy of the data produces exactly the observed results. Linearizability is
 * <em>local</em> (compositional): a history is linearizable if and only if its sub-history for
 * each key is, so we check keys independently, which keeps the search small.
 */
public final class Linearizability {

    /** One completed client operation as observed by the client. */
    public record Op(int client, Command.Op kind, String key, String value, String expected,
                     long invoke, long response, Result result) {}

    private Linearizability() {}

    /** Returns null if the history is linearizable, otherwise a description of the first bad key. */
    public static String check(List<Op> history) {
        Map<String, List<Op>> byKey = new HashMap<>();
        for (Op op : history) byKey.computeIfAbsent(op.key(), k -> new ArrayList<>()).add(op);
        for (Map.Entry<String, List<Op>> e : byKey.entrySet()) {
            if (!checkKey(e.getValue())) return "history for key '" + e.getKey() + "' is not linearizable";
        }
        return null;
    }

    // ---------------- the sequential model: one key holding a string (or nothing) ----------------

    private record Step(boolean ok, String state) {}

    private static Step step(String state, Op op) {
        Result.Status s = op.result().status();
        return switch (op.kind()) {
            case GET -> new Step(
                    (s == Result.Status.NOT_FOUND && state == null)
                            || (s == Result.Status.OK && Objects.equals(state, op.result().value())), state);
            case PUT -> new Step(s == Result.Status.OK, op.value());
            case APPEND -> new Step(s == Result.Status.OK, state == null ? op.value() : state + op.value());
            case DELETE -> new Step(s == Result.Status.OK, null);
            case CAS -> Objects.equals(state, op.expected())
                    ? new Step(s == Result.Status.OK, op.value())
                    : new Step(s == Result.Status.CAS_MISMATCH, state);
        };
    }

    // ---------------- the search ----------------

    /** A doubly linked list node: either the invocation or the response of operation {@code op}. */
    private static final class Node {
        final boolean invocation;
        final int op;
        Node match;                 // invocation <-> response
        Node prev, next;

        Node(boolean invocation, int op) { this.invocation = invocation; this.op = op; }
    }

    private record Frame(Node node, String state) {}

    private record CacheKey(BitSet linearized, String state) {}

    private static boolean checkKey(List<Op> ops) {
        // Build the event list ordered by time; at equal times, invocations come first, so
        // operations that touch at a single instant are treated as concurrent (the safe choice).
        record Event(long time, boolean invocation, int op) {}
        List<Event> events = new ArrayList<>();
        for (int i = 0; i < ops.size(); i++) {
            events.add(new Event(ops.get(i).invoke(), true, i));
            events.add(new Event(ops.get(i).response(), false, i));
        }
        events.sort(Comparator.comparingLong(Event::time).thenComparing(ev -> !ev.invocation()));

        Node head = new Node(false, -1);
        Node tail = head;
        Node[] invocations = new Node[ops.size()];
        for (Event ev : events) {
            Node node = new Node(ev.invocation(), ev.op());
            if (ev.invocation()) invocations[ev.op()] = node;
            else { node.match = invocations[ev.op()]; invocations[ev.op()].match = node; }
            tail.next = node;
            node.prev = tail;
            tail = node;
        }

        BitSet linearized = new BitSet(ops.size());
        Set<CacheKey> seen = new HashSet<>();
        Deque<Frame> stack = new ArrayDeque<>();
        String state = null;
        Node entry = head.next;
        while (head.next != null) {
            if (entry.invocation) {
                Step st = step(state, ops.get(entry.op));
                if (st.ok()) {
                    BitSet next = (BitSet) linearized.clone();
                    next.set(entry.op);
                    if (seen.add(new CacheKey(next, st.state()))) {
                        // Tentatively linearize this operation here.
                        stack.push(new Frame(entry, state));
                        state = st.state();
                        linearized.set(entry.op);
                        lift(entry);
                        entry = head.next;
                        continue;
                    }
                }
                entry = entry.next;                               // try a later concurrent invocation
            } else {
                // Reached the response of an operation we have not linearized: backtrack.
                if (stack.isEmpty()) return false;
                Frame f = stack.pop();
                state = f.state();
                linearized.clear(f.node().op);
                unlift(f.node());
                entry = f.node().next;
            }
        }
        return true;
    }

    private static void lift(Node inv) {
        inv.prev.next = inv.next;
        if (inv.next != null) inv.next.prev = inv.prev;
        Node resp = inv.match;
        resp.prev.next = resp.next;
        if (resp.next != null) resp.next.prev = resp.prev;
    }

    private static void unlift(Node inv) {
        Node resp = inv.match;
        resp.prev.next = resp;
        if (resp.next != null) resp.next.prev = resp;
        inv.prev.next = inv;
        if (inv.next != null) inv.next.prev = inv;
    }
}
