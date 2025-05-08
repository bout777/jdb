package com.jdb.recovery.logs;

import com.jdb.Engine;
import com.jdb.catalog.Schema;
import com.jdb.common.PageHelper;
import com.jdb.recovery.LogType;
import com.jdb.storage.Page;
import com.jdb.table.DataPage;
import com.jdb.table.PagePointer;
import com.jdb.table.RowData;
import com.jdb.table.Table;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class DeleteLog extends LogRecord {
    private static final int HEADER_SIZE = Long.BYTES * 2 + PagePointer.SIZE + Integer.BYTES;
    long xid;
    long prevLsn;
    PagePointer ptr;
    int len;
    byte[] image;

    public DeleteLog(long xid, long prevLsn, PagePointer ptr, byte[] image) {
        super(LogType.DELETE);
        this.xid = xid;
        this.prevLsn = prevLsn;
        this.ptr = ptr;
        this.len = image.length;
        this.image = image;
    }

    public static LogRecord deserializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset);
        long xid = buffer.getLong();
        long prevLsn = buffer.getLong();
        long pid = buffer.getLong();
        int pof = buffer.getInt();
        int len = buffer.getInt();
        byte[] image = new byte[len];
        buffer.get(image);
        return new DeleteLog(xid, prevLsn, new PagePointer(pid, pof), image);
    }

    @Override
    protected int serializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset)
                .putLong(xid)
                .putLong(prevLsn)
                .putLong(ptr.pid)
                .putInt(ptr.sid)
                .putInt(len)
                .put(image);

        return buffer.position();
    }

    @Override
    public long getPageId() {
        return ptr.pid;
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
        return HEADER_SIZE + image.length;
    }

    @Override
    public LogType getType() {
        return LogType.DELETE;
    }


    @Override
    public void redo(Engine engine) {
        //redo时可以根据pageLsn跟lsn比较，来判断是否需要redo，所以直接物理删除
        var bp = engine.getBufferPool();
        var rm = engine.getRecoveryManager();
        int fid = PageHelper.getFid(ptr.pid);
        var schema = engine.getTableManager().getTableSchema(fid);
        Page page = bp.getPage(ptr.pid);
        DataPage dataPage = new DataPage(page, bp, rm, schema);
        dataPage.deleteRecord(ptr.sid);
    }

    @Override
    public void undo(Engine engine) {
        //同insertLog，如果直接在该页插入，可能会破坏有序性
//        int fid = PageHelper.getFid(ptr.pid);
        Table table = engine.getTableManager().getTable(PageHelper.getFid(ptr.pid));
        Schema schema = table.getSchema();
        var rowData = RowData.deserialize(ByteBuffer.wrap(image), 0, schema);
        table.insertRecord(rowData, true, false);

    }

    @Override
    public String toString() {
        return "DeleteLog{" +
                "xid=" + xid +
                ", prevLsn=" + prevLsn +
                ", ptr=" + ptr +
                ", len=" + len +
                ", image=" + Arrays.toString(image) +
                '}';
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        var that = (DeleteLog) o;
        return xid == that.xid &&
                prevLsn == that.prevLsn &&
                ptr.equals(that.ptr) &&
                Arrays.equals(image, that.image);
    }
}
