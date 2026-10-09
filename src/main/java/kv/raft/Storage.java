package kv.raft;

import java.util.List;

/**
 * Durable Raft state. Every method must return only after the data is durable
 * (for {@link FileStorage}: written and fsynced). Raft calls these methods BEFORE it sends any
 * message that depends on them, which is the "persist before you reply" rule.
 */
public interface Storage {
    record HardState(long term, int votedFor) {}

    record Snapshot(long index, long term, byte[] data) {
        public static final Snapshot EMPTY = new Snapshot(0, 0, new byte[0]);
    }

    HardState hardState();

    void saveHardState(long term, int votedFor);

    /** Latest snapshot, or {@link Snapshot#EMPTY} if none. */
    Snapshot snapshot();

    /** Log entries after the snapshot index, in order. */
    List<Entry> entries();

    /** Append entries; the caller guarantees they directly follow the current last entry. */
    void append(List<Entry> entries);

    /** Delete every entry whose index is >= {@code index} (conflict truncation). */
    void truncateFrom(long index);

    /** Atomically replace the snapshot and keep only {@code retained} entries after it. */
    void installSnapshot(Snapshot snapshot, List<Entry> retained);
}
