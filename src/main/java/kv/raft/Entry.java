package kv.raft;

/**
 * One log slot: the command plus the term of the leader that created it.
 * (term, index) identifies an entry uniquely across the cluster.
 * A null command is a no-op, which a new leader appends to commit something in its own term.
 */
public record Entry(long term, long index, byte[] command) {
    public boolean isNoop() {
        return command == null;
    }
}
