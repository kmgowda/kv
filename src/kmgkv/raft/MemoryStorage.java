package kmgkv.raft;

import java.util.ArrayList;
import java.util.List;

/**
 * Storage kept in memory but treated as "durable" by the simulator: when a simulated node
 * crashes, its RaftNode and state machine are discarded, but this object survives and is used
 * to rebuild the node. That models a disk whose writes completed before the crash.
 */
public final class MemoryStorage implements Storage {
    private HardState hardState = new HardState(0, -1);
    private Snapshot snapshot = Snapshot.EMPTY;
    private final List<Entry> entries = new ArrayList<>();

    @Override public synchronized HardState hardState() { return hardState; }

    @Override public synchronized void saveHardState(long term, int votedFor) {
        hardState = new HardState(term, votedFor);
    }

    @Override public synchronized Snapshot snapshot() { return snapshot; }

    @Override public synchronized List<Entry> entries() { return List.copyOf(entries); }

    @Override public synchronized void append(List<Entry> es) {
        for (Entry e : es) {
            long expected = (entries.isEmpty() ? snapshot.index() : entries.get(entries.size() - 1).index()) + 1;
            if (e.index() != expected) {
                throw new IllegalStateException("non-contiguous append: " + e.index() + " expected " + expected);
            }
            entries.add(e);
        }
    }

    @Override public synchronized void truncateFrom(long index) {
        entries.removeIf(e -> e.index() >= index);
    }

    @Override public synchronized void installSnapshot(Snapshot s, List<Entry> retained) {
        snapshot = s;
        entries.clear();
        entries.addAll(retained);
    }
}
