package com.jdb.recovery.logs;

import com.jdb.exception.DuplicateInsertException;
import com.jdb.recovery.LogRecord;
import com.jdb.recovery.LogType;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Page;
import com.jdb.table.DataPage;
import com.jdb.table.RecordID;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class InsertLog extends LogRecord {
    long xid;
    long prevLsn;
    RecordID rid;
    int len;
    byte[] image;
    private static final int HEADER_SIZE = LogRecord.HEADER_SIZE + Long.BYTES * 2 + RecordID.SIZE + Integer.BYTES;

    public InsertLog(long xid, long prevLsn, RecordID rid, byte[] image) {
        super(LogType.INSERT);
        this.xid = xid;
        this.prevLsn = prevLsn;
        this.rid = rid;
        this.len = image.length;
        this.image = image;
    }

    @Override
    public int serialize(ByteBuffer buffer, int offset) {
        buffer.position(offset)
                .put((byte) getType().getValue())
                .putLong(xid)
                .putLong(prevLsn)
                .putLong(rid.pid)
                .putInt(rid.slotId)
                .putInt(len)
                .put(image);

        return buffer.position();
    }

    public static LogRecord deserialize(ByteBuffer buffer, int offset) {
        buffer.position(offset);
        long xid = buffer.getLong();
        long prevLsn = buffer.getLong();
        long pid = buffer.getLong();
        int slotId = buffer.getInt();
        int len = buffer.getInt();
        byte[] image = new byte[len];
        buffer.get(image);
        return new InsertLog(xid,prevLsn, new RecordID(pid, slotId), image);
    }


    @Override
    public long getPageId() {
        return rid.pid;
    }

    @Override
    public long getXid() {
        return xid;
    }

    @Override
    public int getSize() {
        return HEADER_SIZE + image.length;
    }


    @Override
    public LogType getType() {
        return LogType.INSERT;
    }

    @Override
    public void redo() {
        Page page = BufferPool.getInstance().getPage(rid.pid);
        DataPage dataPage = new DataPage(page);
        try {
            dataPage.insertRecord(rid.slotId,image);
        }catch (DuplicateInsertException e){
            //record has existed, do nothing
        }
    }

    @Override
    public void undo() {

    }

    @Override
    public String toString() {
        return "InsertLog{" +
                "xid=" + xid +
                ", prevLsn=" + prevLsn +
                ", rid=" + rid +
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
                rid.equals(insertLog.rid) &&
                Arrays.equals(image, insertLog.image);
    }
}
