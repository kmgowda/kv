package kmgkv.raft;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * File-backed storage, written for clarity rather than speed.
 *
 * <pre>
 *   hardstate  : [crc][term][votedFor]                       rewritten atomically (tmp + fsync + rename)
 *   snapshot   : [crc][generation][index][term][data][retained entries]   rewritten atomically
 *   wal        : header [generation], then records [length][crc][type][payload]   append + fsync
 * </pre>
 *
 * Crash safety:
 * <ul>
 *   <li>A torn or corrupt record at the end of the WAL (crash mid-write) is detected by its
 *       length/CRC and cut off; everything before it is kept.</li>
 *   <li>Installing a snapshot writes the new snapshot file (generation g+1, including the entries
 *       that follow it) atomically, then starts a fresh WAL with header g+1. If the process crashes
 *       between the two steps, the old WAL still says generation g and is ignored on restart, so
 *       old and new log contents are never mixed.</li>
 * </ul>
 * Production engines use segmented WALs, batched fsync (group commit), and incremental snapshots.
 */
public final class FileStorage implements Storage {
    private static final byte ENTRY = 1;
    private static final byte TRUNCATE = 2;

    private final Path dir;
    private HardState hardState = new HardState(0, -1);
    private Snapshot snapshot = Snapshot.EMPTY;
    private long generation = 0;
    private final List<Entry> entries = new ArrayList<>();   // in-memory mirror of the durable log
    private FileChannel wal;

    public FileStorage(Path dir) {
        this.dir = dir;
        try {
            Files.createDirectories(dir);
            loadHardState();
            List<Entry> retained = loadSnapshot();
            entries.addAll(retained);
            openWal();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // ---------------- public API ----------------

    @Override public synchronized HardState hardState() { return hardState; }

    @Override public synchronized void saveHardState(long term, int votedFor) {
        writeAtomically("hardstate", out -> { out.writeLong(term); out.writeInt(votedFor); });
        hardState = new HardState(term, votedFor);
    }

    @Override public synchronized Snapshot snapshot() { return snapshot; }

    @Override public synchronized List<Entry> entries() { return List.copyOf(entries); }

    @Override public synchronized void append(List<Entry> es) {
        if (es.isEmpty()) return;
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        for (Entry e : es) {
            long expected = lastIndex() + 1;
            if (e.index() != expected) throw new IllegalStateException("non-contiguous append " + e.index());
            buf.writeBytes(record(ENTRY, out -> writeEntry(out, e)));
            entries.add(e);
        }
        appendToWal(buf.toByteArray());
    }

    @Override public synchronized void truncateFrom(long index) {
        appendToWal(record(TRUNCATE, out -> out.writeLong(index)));
        entries.removeIf(e -> e.index() >= index);
    }

    @Override public synchronized void installSnapshot(Snapshot s, List<Entry> retained) {
        long next = generation + 1;
        writeAtomically("snapshot", out -> {                    // step 1: snapshot + retained entries
            out.writeLong(next);
            out.writeLong(s.index());
            out.writeLong(s.term());
            out.writeInt(s.data().length);
            out.write(s.data());
            out.writeInt(retained.size());
            for (Entry e : retained) writeEntry(out, e);
        });
        generation = next;
        snapshot = s;
        entries.clear();
        entries.addAll(retained);
        try {                                                   // step 2: fresh WAL for the new generation
            wal.close();
            writeAtomically("wal", out -> out.writeLong(next));
            wal = FileChannel.open(dir.resolve("wal"), StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public synchronized void close() throws IOException { wal.close(); }

    // ---------------- loading ----------------

    private void loadHardState() throws IOException {
        byte[] body = readChecked("hardstate");
        if (body == null) return;
        DataInputStream in = new DataInputStream(new java.io.ByteArrayInputStream(body));
        hardState = new HardState(in.readLong(), in.readInt());
    }

    private List<Entry> loadSnapshot() throws IOException {
        byte[] body = readChecked("snapshot");
        if (body == null) return List.of();
        DataInputStream in = new DataInputStream(new java.io.ByteArrayInputStream(body));
        generation = in.readLong();
        long index = in.readLong(), term = in.readLong();
        byte[] data = in.readNBytes(in.readInt());
        snapshot = new Snapshot(index, term, data);
        int n = in.readInt();
        List<Entry> retained = new ArrayList<>(n);
        for (int i = 0; i < n; i++) retained.add(readEntry(in));
        return retained;
    }

    private void openWal() throws IOException {
        Path p = dir.resolve("wal");
        if (!Files.exists(p)) writeAtomically("wal", out -> out.writeLong(generation));
        wal = FileChannel.open(p, StandardOpenOption.READ, StandardOpenOption.WRITE);
        ByteBuffer header = ByteBuffer.allocate(8);
        wal.read(header, 0);
        long walGeneration = header.flip().getLong();
        if (walGeneration != generation) {                      // crash between snapshot steps: stale WAL
            wal.close();
            writeAtomically("wal", out -> out.writeLong(generation));
            wal = FileChannel.open(p, StandardOpenOption.READ, StandardOpenOption.WRITE);
        } else {
            long goodEnd = replayRecords();
            if (goodEnd < wal.size()) {                         // torn/corrupt tail: cut it off
                wal.truncate(goodEnd);
                wal.force(true);
            }
        }
        wal.position(wal.size());
    }

    /** Replays WAL records into {@link #entries}; returns the offset just after the last valid record. */
    private long replayRecords() throws IOException {
        long pos = 8;
        long size = wal.size();
        while (pos + 9 <= size) {
            ByteBuffer head = ByteBuffer.allocate(8);
            wal.read(head, pos);
            head.flip();
            int length = head.getInt();
            int crc = head.getInt();
            if (length < 1 || pos + 8 + length > size) break;   // incomplete record
            ByteBuffer body = ByteBuffer.allocate(length);
            wal.read(body, pos + 8);
            byte[] bytes = body.array();
            if (crc32(bytes) != crc) break;                     // corrupt record
            DataInputStream in = new DataInputStream(new java.io.ByteArrayInputStream(bytes));
            byte type = in.readByte();
            if (type == ENTRY) {
                Entry e = readEntry(in);
                if (e.index() > snapshot.index()) {
                    entries.removeIf(x -> x.index() >= e.index()); // defensive: later record wins
                    entries.add(e);
                }
            } else if (type == TRUNCATE) {
                long from = in.readLong();
                entries.removeIf(x -> x.index() >= from);
            }
            pos += 8 + length;
        }
        return pos;
    }

    // ---------------- helpers ----------------

    private long lastIndex() {
        return entries.isEmpty() ? snapshot.index() : entries.get(entries.size() - 1).index();
    }

    private interface Writer { void write(DataOutputStream out) throws IOException; }

    private static void writeEntry(DataOutputStream out, Entry e) throws IOException {
        out.writeLong(e.term());
        out.writeLong(e.index());
        if (e.command() == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(e.command().length);
            out.write(e.command());
        }
    }

    private static Entry readEntry(DataInputStream in) throws IOException {
        long term = in.readLong(), index = in.readLong();
        int len = in.readInt();
        byte[] cmd = len < 0 ? null : in.readNBytes(len);
        if (cmd != null && cmd.length != len) throw new EOFException();
        return new Entry(term, index, cmd);
    }

    private static byte[] record(byte type, Writer w) {
        byte[] body = bytes(out -> { out.writeByte(type); w.write(out); });
        ByteBuffer buf = ByteBuffer.allocate(8 + body.length);
        buf.putInt(body.length).putInt(crc32(body)).put(body);
        return buf.array();
    }

    private void appendToWal(byte[] data) {
        try {
            wal.write(ByteBuffer.wrap(data));
            wal.force(false);                                   // durable before Raft replies
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Writes [crc][body] to a temp file, fsyncs it, renames it over the target, fsyncs the directory. */
    private void writeAtomically(String name, Writer w) {
        byte[] body = bytes(w);
        ByteBuffer buf = ByteBuffer.allocate(4 + body.length).putInt(crc32(body)).put(body);
        if (name.equals("wal")) buf = ByteBuffer.wrap(body);   // the WAL header is plain (records carry CRCs)
        Path tmp = dir.resolve(name + ".tmp");
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            ch.write(buf.rewind());
            ch.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try {
            Files.move(tmp, dir.resolve(name), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            try (FileChannel d = FileChannel.open(dir, StandardOpenOption.READ)) {
                d.force(true);                                  // make the rename itself durable
            } catch (IOException ignored) {
                // some platforms do not allow fsync on a directory
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private byte[] readChecked(String name) throws IOException {
        Path p = dir.resolve(name);
        if (!Files.exists(p)) return null;
        byte[] all = Files.readAllBytes(p);
        if (all.length < 4) throw new IOException(name + " is truncated");
        ByteBuffer buf = ByteBuffer.wrap(all);
        int crc = buf.getInt();
        byte[] body = new byte[all.length - 4];
        buf.get(body);
        if (crc32(body) != crc) throw new IOException(name + " failed its checksum");
        return body;
    }

    private static byte[] bytes(Writer w) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bos)) {
            w.write(out);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bos.toByteArray();
    }

    private static int crc32(byte[] b) {
        CRC32 c = new CRC32();
        c.update(b);
        return (int) c.getValue();
    }
}
