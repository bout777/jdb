package com.jdb.recovery.logs;

import com.jdb.Engine;
import com.jdb.recovery.LogType;
import com.jdb.storage.Page;
import com.jdb.storage.PageType;
import com.jdb.table.IndexPage;

import java.nio.ByteBuffer;

public class IndexPageInitLog extends LogRecord {
    long xid;
    long prevLsn;
    long pid;

    public IndexPageInitLog(long xid, long prevLsn, long pid) {
        super(LogType.INDEX_PAGE_INIT);
        this.xid = xid;
        this.prevLsn = prevLsn;
        this.pid = pid;
    }

    public static LogRecord deserializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset);
        long xid = buffer.getLong();
        long prevLsn = buffer.getLong();
        long pid = buffer.getLong();
        return new IndexPageInitLog(xid, prevLsn, pid);
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
        return LogType.INDEX_PAGE_INIT;
    }

    @Override
    public void redo(Engine engine) {
        Page page = engine.getBufferPool().getPage(pid);
        var buffer = page.getBuffer();
        buffer.put(IndexPage.PAGE_TYPE_OFFSET, PageType.INDEX_PAGE);
    }

    @Override
    public void undo(Engine engine) {

    }


    @Override
    public String toString() {
        return "IndexPageInitLog{" +
                "xid=" + xid +
                ", prevLsn=" + prevLsn +
                ", pid=" + pid +
                '}';
    }


}
