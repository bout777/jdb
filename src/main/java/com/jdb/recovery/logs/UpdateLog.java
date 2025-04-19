package com.jdb.recovery.logs;

import com.jdb.Engine;
import com.jdb.recovery.LogType;
import com.jdb.storage.Page;
import com.jdb.table.PagePointer;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * 用于记录非主键值的更新
 */
public class UpdateLog extends LogRecord {
    private static final int RID_OFFSET = 1;
    private static final int XID_OFFSET = RID_OFFSET + PagePointer.SIZE;
    private static final int PREV_LSN_OFFSET = XID_OFFSET + Integer.BYTES;
    private static final int HEADER_SIZE = Byte.BYTES + Long.BYTES + Integer.BYTES + Long.BYTES + Short.BYTES + Short.BYTES;
    private long xid;
    private int pid;
    private long prevLsn;
    private short offset;
    private byte[] oldData;
    private byte[] newData;

    public UpdateLog(long xid, int pid, long prevLsn, short offset, byte[] oldData, byte[] newData) {
        super(LogType.UPDATE);
        this.xid = xid;
        this.pid = pid;
        this.prevLsn = prevLsn;
        this.offset = offset;
        this.oldData = oldData;
        this.newData = newData;
    }


    public static LogRecord deserializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset);
        long xid = buffer.getLong();
        int pid = buffer.getInt();
        long prevLsn = buffer.getLong();
        short pageOffset = buffer.getShort();
        short len = buffer.getShort();
        byte[] oldData = new byte[len];
        byte[] newData = new byte[len];
        buffer.get(oldData).get(newData);
        return new UpdateLog(xid, pid, prevLsn, pageOffset, oldData, newData);
    }

    @Override
    public long getPrevLsn() {
        return prevLsn;
    }

    @Override
    protected int serializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset)
                .putLong(xid)
                .putInt(pid)
                .putLong(prevLsn)
                .putShort(this.offset)
                .putShort((short) oldData.length)
                .put(oldData).put(newData);
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
    protected int getPayloadSize() {
        return HEADER_SIZE + oldData.length + newData.length;
    }

    @Override
    public LogType getType() {
        return LogType.UPDATE;
    }

    /**
     * 重做更新
     * 从bufferpool中取出数据页，将对应的记录更新
     * 更新默认不会改变改记录在簇集索引的位置
     * 但是会修改二级索引
     */
    @Override
    public void redo(Engine engine) {
        var bp = engine.getBufferPool();
        Page page = bp.getPage(pid);
        ByteBuffer buffer = page.getBuffer();
        buffer.put(offset, newData);
    }

    @Override
    public void undo(Engine engine) {
//        Page page = BufferPool.getInstance().getPage(pid);
//        ByteBuffer buffer = page.getBuffer();
//        buffer.put(offset, oldData);
    }

    @Override
    public String toString() {
        return "UpdateLog{" +
                "xid=" + xid +
                ", pid=" + pid +
                ", prevLsn=" + prevLsn +
                ", offset=" + offset +
                ", oldData=" + Arrays.toString(oldData) +
                ", newData=" + Arrays.toString(newData) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UpdateLog updateLog = (UpdateLog) o;
        return xid == updateLog.xid &&
                pid == updateLog.pid &&
                prevLsn == updateLog.prevLsn &&
                offset == updateLog.offset &&
                java.util.Arrays.equals(oldData, updateLog.oldData) &&
                java.util.Arrays.equals(newData, updateLog.newData);
    }

}
