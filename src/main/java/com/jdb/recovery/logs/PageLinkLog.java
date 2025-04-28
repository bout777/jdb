package com.jdb.recovery.logs;

import com.jdb.Engine;
import com.jdb.recovery.LogType;

import java.nio.ByteBuffer;
import java.util.Objects;

public class PageLinkLog extends LogRecord{
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
    protected int getPayloadSize() {
        return Long.BYTES*5;
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

    @Override
    public LogType getType() {
        return LogType.PAGE_LINK;
    }
    @Override
    public void redo(Engine engine) {

    }
    @Override
    public void undo(Engine engine) {

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
