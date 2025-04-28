package com.jdb.recovery.logs;

import com.jdb.Engine;
import com.jdb.recovery.LogType;
import com.jdb.storage.Page;
import com.jdb.table.DataPage;

import java.nio.ByteBuffer;
import java.util.Objects;

import static com.jdb.common.Constants.NULL_PAGE_ID;
import static com.jdb.common.Constants.PAGE_SIZE;

public class DataPageInitLog extends LogRecord {
    long xid;
    long prevLsn;
    long pid;

    public DataPageInitLog(long xid, long prevLsn, long pid) {
        super(LogType.DATA_PAGE_INIT);
        this.xid = xid;
        this.prevLsn = prevLsn;
        this.pid = pid;
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

    public static LogRecord deserializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset);
        long xid = buffer.getLong();
        long prevLsn = buffer.getLong();
        long pid = buffer.getLong();
        return new DataPageInitLog(xid, prevLsn, pid);
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
        return LogType.DATA_PAGE_INIT;
    }

    @Override
    public void redo(Engine engine) {
        Page page = engine.getBufferPool().getPage(pid);
        var buffer = page.getBuffer();
        buffer.putLong(DataPage.NEXT_PAGE_ID_OFFSET, NULL_PAGE_ID);
        buffer.putInt(DataPage.LOWER_OFFSET, DataPage.HEADER_SIZE);
        buffer.putInt(DataPage.UPPER_OFFSET, PAGE_SIZE);
        buffer.put(DataPage.PAGE_TYPE_OFFSET, (byte) 2);
    }

    @Override
    public void undo(Engine engine) {

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataPageInitLog that = (DataPageInitLog) o;
        return xid == that.xid &&
                prevLsn == that.prevLsn &&
                pid == that.pid;
    }

    @Override
    public int hashCode() {
        return Objects.hash(xid, prevLsn, pid);
    }

    @Override
    public String toString() {
        return "DataPageInitLog{" +
                "xid=" + xid +
                ", prevLsn=" + prevLsn +
                ", pid=" + pid +
                '}';
    }


}
