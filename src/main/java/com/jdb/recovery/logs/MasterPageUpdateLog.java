package com.jdb.recovery.logs;

import com.jdb.Engine;
import com.jdb.recovery.LogType;
import com.jdb.storage.Page;
import com.jdb.table.MasterPage;

import java.nio.ByteBuffer;
import java.util.Objects;

public class MasterPageUpdateLog extends LogRecord {

    long xid;
    long prevLsn;
    long pid;
    long beforeRootPageId;
    long afterRootPageId;

    public MasterPageUpdateLog(long xid, long prevLsn, long pid, long beforeRootPageId, long afterRootPageId) {
        super(LogType.MASTER_PAGE_UPDATE);
        this.xid = xid;
        this.prevLsn = prevLsn;
        this.pid = pid;
        this.beforeRootPageId = beforeRootPageId;
        this.afterRootPageId = afterRootPageId;
    }

    @Override
    protected int serializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset)
                .putLong(xid)
                .putLong(prevLsn)
                .putLong(pid)
                .putLong(beforeRootPageId)
                .putLong(afterRootPageId);
        return buffer.position();
    }

    public static LogRecord deserializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset);
        long xid = buffer.getLong();
        long prevLsn = buffer.getLong();
        long pid = buffer.getLong();
        long beforeRootPageId = buffer.getLong();
        long afterRootPageId = buffer.getLong();
        return new MasterPageUpdateLog(xid, prevLsn, pid, beforeRootPageId, afterRootPageId);
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
        return Long.BYTES * 5;
    }


    @Override
    public LogType getType() {
        return LogType.MASTER_PAGE_UPDATE;
    }

    @Override
    public void redo(Engine engine) {
//        System.out.println("on redo: "+this);
        var bp = engine.getBufferPool();
        var bf = bp.getPage(pid).getBuffer();
        bf.putLong(MasterPage.ROOT_OFFSET, afterRootPageId);
    }

    @Override
    public void undo(Engine engine) {
    }

    @Override
    public String toString() {
        return "MasterPageUpdateLog{" +
                "xid=" + xid +
                ", prevLsn=" + prevLsn +
                ", pid=" + pid +
                ", beforeRootPageId=" + beforeRootPageId +
                ", afterRootPageId=" + afterRootPageId +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MasterPageUpdateLog)) return false;
        MasterPageUpdateLog that = (MasterPageUpdateLog) o;
        return xid == that.xid &&
                prevLsn == that.prevLsn &&
                pid == that.pid &&
                beforeRootPageId == that.beforeRootPageId &&
                afterRootPageId == that.afterRootPageId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(xid, prevLsn, pid, beforeRootPageId, afterRootPageId);
    }
}
