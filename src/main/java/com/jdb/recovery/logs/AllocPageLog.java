package com.jdb.recovery.logs;

import com.jdb.Engine;
import com.jdb.recovery.LogType;
import com.jdb.storage.Page;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.Objects;

public class AllocPageLog extends LogRecord {
    long xid;
    long prevLsn;
    long pid;

    public AllocPageLog(long xid, long prevLsn, long pid) {
        super(LogType.ALLOC_PAGE);
        this.xid = xid;
        this.prevLsn = prevLsn;
        this.pid = pid;
    }

    public static LogRecord deserializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset);
        long xid = buffer.getLong();
        long prevLsn = buffer.getLong();
        long pid = buffer.getLong();
        return new AllocPageLog(xid, prevLsn, pid);
    }

    @Override
    protected int getPayloadSize() {
        return Long.BYTES * 3;
    }

    @Override
    protected int serializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset)
                .putLong(xid)
                .putLong(prevLsn)
                .putLong(pid);
        return buffer.position();
    }

    @Override
    public long getPageId() {
        return pid;
    }

    @Override
    public long getXid() {
        return xid;
    }

    @Override
    public long getPrevLsn() {
        return prevLsn;
    }

    @Override
    public LogType getType() {
        return LogType.ALLOC_PAGE;
    }

    @Override
    public void redo(Engine engine) {
        var bp = engine.getBufferPool();
        var disk = engine.getDisk();
        try {
            bp.getPage(pid);
        } catch (NoSuchElementException e) {
            disk.writePage(new Page(pid));
        }
    }

    @Override
    public void undo(Engine engine) {
    }

    @Override
    public String toString() {
        return String.format("AllocPageLog{xid=%d, prevLsn=%d, pid=%d}", xid, prevLsn, pid);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AllocPageLog that)) return false;
        return xid == that.xid &&
                prevLsn == that.prevLsn &&
                pid == that.pid;
    }

    @Override
    public int hashCode() {
        return Objects.hash(xid, prevLsn, pid);
    }

}
