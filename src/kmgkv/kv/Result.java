package kmgkv.kv;

/** The outcome of an operation, as returned to the client. */
public record Result(Status status, String value) {
    public enum Status {
        OK,
        NOT_FOUND,
        CAS_MISMATCH,
        /** Not the leader, or leadership changed before the operation finished: retry elsewhere. */
        WRONG_LEADER,
        /** The client sent an older sequence number than one already executed: a client bug. */
        STALE_SEQUENCE
    }

    public static Result ok(String value) { return new Result(Status.OK, value); }

    public static Result of(Status s) { return new Result(s, null); }

    /** Final answers end the client's retry loop; WRONG_LEADER does not. */
    public boolean isFinal() { return status != Status.WRONG_LEADER; }
}
