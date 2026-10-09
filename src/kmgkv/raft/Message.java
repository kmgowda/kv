package kmgkv.raft;

import java.util.List;

/** All Raft RPCs. Every message carries the sender's current term. */
public sealed interface Message {
    long term();

    record RequestVote(long term, int candidate, long lastLogIndex, long lastLogTerm) implements Message {}

    record RequestVoteReply(long term, boolean granted) implements Message {}

    /**
     * Replication and heartbeat. {@code ctx} is the leader's read-context counter at send time;
     * followers echo it, which lets the leader confirm leadership for ReadIndex reads.
     */
    record AppendEntries(long term, int leader, long prevLogIndex, long prevLogTerm,
                         List<Entry> entries, long leaderCommit, long ctx) implements Message {}

    /**
     * On success, {@code matchIndex} is the last index known to match the leader.
     * On failure, {@code conflictIndex}/{@code conflictTerm} help the leader skip back quickly
     * ({@code conflictTerm == -1} means "my log is too short or already compacted there").
     */
    record AppendEntriesReply(long term, boolean success, long matchIndex,
                              long conflictIndex, long conflictTerm, long ctx) implements Message {}

    /** Whole-snapshot transfer (production systems send it in chunks). */
    record InstallSnapshot(long term, int leader, long lastIncludedIndex, long lastIncludedTerm,
                           byte[] data, long ctx) implements Message {}

    record InstallSnapshotReply(long term, long matchIndex, long ctx) implements Message {}
}
