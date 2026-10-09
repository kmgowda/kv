package kv.store;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * A client operation. {@code (clientId, seq)} identifies it; a client retries with the SAME seq
 * until it gets an answer, and only then uses seq + 1 (one outstanding request per client).
 * GET is served through ReadIndex and never written to the log.
 */
public record Command(long clientId, long seq, Op op, String key, String value, String expected) {
    public enum Op { GET, PUT, APPEND, DELETE, CAS }

    public static Command get(long clientId, long seq, String key) {
        return new Command(clientId, seq, Op.GET, key, null, null);
    }

    public byte[] encode() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeLong(clientId);
            out.writeLong(seq);
            out.writeByte(op.ordinal());
            writeNullable(out, key);
            writeNullable(out, value);
            writeNullable(out, expected);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bos.toByteArray();
    }

    public static Command decode(byte[] bytes) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            return new Command(in.readLong(), in.readLong(), Op.values()[in.readByte()],
                    readNullable(in), readNullable(in), readNullable(in));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static void writeNullable(DataOutputStream out, String s) throws IOException {
        out.writeBoolean(s != null);
        if (s != null) out.writeUTF(s);
    }

    static String readNullable(DataInputStream in) throws IOException {
        return in.readBoolean() ? in.readUTF() : null;
    }
}
