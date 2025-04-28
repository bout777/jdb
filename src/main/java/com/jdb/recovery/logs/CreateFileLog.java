package com.jdb.recovery.logs;

import com.jdb.Engine;
import com.jdb.recovery.LogType;

import java.nio.ByteBuffer;

public class CreateFileLog extends LogRecord{
    long xid;
    long prevLsn;
    int fid;
    short len;
    //utf-8
    byte[] filePath;

    public CreateFileLog(long xid, long prevLsn, int fid, String filePath) {
        super(LogType.CREATE_FILE);
        this.xid = xid;
        this.prevLsn = prevLsn;
        this.fid = fid;
        this.filePath = filePath.getBytes();
        this.len = (short) this.filePath.length;
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
        return Long.BYTES*2+Integer.BYTES+Short.BYTES+filePath.length;
    }

    @Override
    protected int serializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset)
                .putLong(xid)
                .putLong(prevLsn)
                .putInt(fid)
                .putShort((short)filePath.length)
                .put(filePath);
        return buffer.position();
    }

    public static LogRecord deserializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset);
        long xid = buffer.getLong();
        long prevLsn = buffer.getLong();
        int fid = buffer.getInt();
        short len = buffer.getShort();
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new CreateFileLog(xid, prevLsn, fid, new String(bytes));
    }


    @Override
    public LogType getType() {
        return LogType.CREATE_FILE;
    }

    @Override
    public void redo(Engine engine) {
        if(!engine.getDisk().fileExist(fid))
            engine.getDisk().putFileMap(fid,new String(filePath));
    }

    @Override
    public String toString() {
        return "CreateFileLog{" +
                "xid=" + xid +
                ", prevLsn=" + prevLsn +
                ", fid=" + fid +
                ", filePath='" + new String(filePath) + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true; // 如果是同一个对象，直接返回 true
        if (o == null || getClass() != o.getClass()) return false; // 如果对象为空或类型不匹配，返回 false

        CreateFileLog that = (CreateFileLog) o; // 将对象强制转换为 CreateFileLog 类型

        // 比较 xid、prevLsn 和 fid 是否相等
        if (xid != that.xid) return false;
        if (prevLsn != that.prevLsn) return false;
        if (fid != that.fid) return false;

        // 比较 filePath 是否相等
        return java.util.Arrays.equals(filePath, that.filePath);
    }
}
