package com.jdb.recovery.logs;

import com.jdb.catalog.Schema;
import com.jdb.common.PageHelper;
import com.jdb.common.Value;
import com.jdb.recovery.LogType;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Page;
import com.jdb.table.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.NoSuchElementException;

public class InsertLog extends LogRecord {
    long xid;
    long prevLsn;
    PagePointer ptr;
    int len;
    byte[] image;
    private static final int HEADER_SIZE = Long.BYTES * 2 + PagePointer.SIZE + Integer.BYTES;

    public InsertLog(long xid, long prevLsn, PagePointer ptr, byte[] image) {
        super(LogType.INSERT);
        this.xid = xid;
        this.prevLsn = prevLsn;
        this.ptr = ptr;
        this.len = image.length;
        this.image = image;
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

    public static LogRecord deserializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset);
        long xid = buffer.getLong();
        long prevLsn = buffer.getLong();
        long pid = buffer.getLong();
        int sid = buffer.getInt();
        int len = buffer.getInt();
        byte[] image = new byte[len];
        buffer.get(image);
        return new InsertLog(xid, prevLsn, new PagePointer(pid, sid), image);
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
        return LogType.INSERT;
    }

    @Override
    public void redo() {
        //redo时可以根据pageLsn跟lsn比较，来判断是否需要redo，所以直接物理插入
        Page page = BufferPool.getInstance().getPage(ptr.pid);
        DataPage dataPage = new DataPage(page);
//        try {
        dataPage.insertRecord(ptr.sid, image);
//        }catch (DuplicateInsertException e){
//            //record has existed, do nothing
//        }
    }

    @Override
    public void undo() {
        //todo undo需要删除，由于已经插入的记录可能被移动到其他地方(页分裂),所以根据页指针删除是不现实的
        int fid = PageHelper.getFid(ptr.pid);
        Table table = TableManager.testTable;
        Schema schema = table.getSchema();
        var rowData = RowData.deserialize(ByteBuffer.wrap(image), 0, schema);
        table.deleteRecord(Value.ofInt(rowData.getPrimaryKey()),true);
    }

    @Override
    public String toString() {
        return "InsertLog{" +
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
        InsertLog insertLog = (InsertLog) o;
        return xid == insertLog.xid &&
                prevLsn == insertLog.prevLsn &&
                ptr.equals(insertLog.ptr) &&
                Arrays.equals(image, insertLog.image);
    }
}
