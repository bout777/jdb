package com.jdb.recovery.logs;

import com.jdb.Engine;
import com.jdb.common.DataType;
import com.jdb.common.Value;
import com.jdb.recovery.LogType;
import com.jdb.table.IndexPage;

import java.nio.ByteBuffer;
import java.util.Objects;

public class IndexEntryInsertLog extends LogRecord {
    long xid;
    long prevLsn;
    long pid;

    long entryPid;
    Value<?> key;

    public IndexEntryInsertLog(long xid, long prevLsn, long pid, Value<?> key, long entryPid) {
        super(LogType.INDEX_PAGE_INSERT);
        this.xid = xid;
        this.prevLsn = prevLsn;
        this.pid = pid;
        this.key = key;
        this.entryPid = entryPid;
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
    protected int getPayloadSize() {
        return Long.BYTES * 4 + Byte.BYTES + key.getBytes();
    }

    protected int serializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset)
                .putLong(xid)
                .putLong(prevLsn)
                .putLong(pid)
                .putLong(entryPid)
                .put((byte) key.getType().ordinal());
        offset = buffer.position();
        return key.serialize(buffer, offset);
    }

    public static LogRecord deserializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset);
        long xid = buffer.getLong();
        long prevLsn = buffer.getLong();
        long pid = buffer.getLong();
        long entryPid = buffer.getLong();
        DataType type = DataType.values()[buffer.get()];
        offset = buffer.position();
        Value<?> key = Value.deserialize(buffer, offset, type);
        return new IndexEntryInsertLog(xid, prevLsn, pid, key, entryPid);
    }

    @Override
    public LogType getType() {
        return LogType.INDEX_PAGE_INSERT;
    }

    @Override
    public void redo(Engine engine) {
        var bp = engine.getBufferPool();
        var rm = engine.getRecoveryManager();
        var page = bp.getPage(pid);
        var indexPage = new IndexPage(page, bp, rm);
        indexPage.insert(key, entryPid, false);
    }

    @Override
    public void undo(Engine engine) {
        //不能直接删,因为这个索引条目指向的页可能已经被其他事务使用
    }

    @Override
    public String toString() {
        return "IndexEntryInsertLog{" +
                "xid=" + xid +
                ", prevLsn=" + prevLsn +
                ", pid=" + pid +
                ", key=" + key +
                ", entryPid=" + entryPid +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IndexEntryInsertLog)) return false;
        IndexEntryInsertLog that = (IndexEntryInsertLog) o;
        return xid == that.xid
                && prevLsn == that.prevLsn
                && pid == that.pid
                && entryPid == that.entryPid
                && Objects.equals(key, that.key)
                && getType() == that.getType();
    }

    @Override
    public int hashCode() {
        return Objects.hash(xid, prevLsn, pid, entryPid, key, getType());
    }
}
