package com.jdb.recovery.logs;

import com.jdb.Engine;
import com.jdb.recovery.LogType;
import com.jdb.storage.Page;
import com.jdb.table.DataPage;
import com.jdb.table.PagePointer;

import java.nio.ByteBuffer;
import java.util.Objects;

public class PageLinkLog extends LogRecord {
    long xid;
    long prevLsn;
    long pid;
    long beforeNextPid;
    long afterNextPid;

    public PageLinkLog(long xid, long prevLsn, long pid, long beforeNextPid, long afterNextPid) {
        super(LogType.PAGE_LINK);
        this.xid = xid;
        this.prevLsn = prevLsn;
        this.pid = pid;
        this.beforeNextPid = beforeNextPid;
        this.afterNextPid = afterNextPid;
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
    protected int serializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset)
                .putLong(xid)
                .putLong(prevLsn)
                .putLong(pid)
                .putLong(beforeNextPid)
                .putLong(afterNextPid);
        return buffer.position();
    }

    public static LogRecord deserializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset);
        long xid = buffer.getLong();
        long prevLsn = buffer.getLong();
        long pid = buffer.getLong();
        long beforeNextPid = buffer.getLong();
        long afterNextPid = buffer.getLong();
        return new PageLinkLog(xid, prevLsn, pid, beforeNextPid, afterNextPid);
    }

    @Override
    public LogType getType() {
        return LogType.PAGE_LINK;
    }

    @Override
    public void redo(Engine engine) {
        var bp = engine.getBufferPool();
        Page page = bp.getPage(pid);
        var bf = page.getBuffer();
        bf.putLong(DataPage.NEXT_PAGE_ID_OFFSET, afterNextPid);
    }

    @Override
    public void undo(Engine engine) {
//        var bp = engine.getBufferPool();
//        Page page = bp.getPage(pid);
//        var bf = page.getBuffer();
//        bf.putLong(DataPage.NEXT_PAGE_ID_OFFSET, beforeNextPid);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PageLinkLog that = (PageLinkLog) o;
        return xid == that.xid &&
                prevLsn == that.prevLsn &&
                pid == that.pid &&
                beforeNextPid == that.beforeNextPid &&
                afterNextPid == that.afterNextPid;
    }

    @Override
    public int hashCode() {
        return Objects.hash(xid, prevLsn, pid, beforeNextPid, afterNextPid);
    }

    @Override
    public String toString() {
        return "PageLinkLog{" +
                "xid=" + xid +
                ", prevLsn=" + prevLsn +
                ", pid=" + pid +
                ", beforeNextPid=" + beforeNextPid +
                ", afterNextPid=" + afterNextPid +
                '}';
    }
}
