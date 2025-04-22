package com.jdb.recovery.logs;

import com.jdb.recovery.LogType;

import java.nio.ByteBuffer;

public class CreateFileLog extends LogRecord{
    long xid;
    long prevLsn;
    int fid;
    short len;
    //utf-8
    String filePath;

    public CreateFileLog(long xid, long prevLsn, int fid, String filePath) {
        super(LogType.CREATE_FILE);
        this.xid = xid;
        this.prevLsn = prevLsn;
        this.fid = fid;
        this.filePath = filePath;
    }


    @Override
    public long getXid() {
        return super.getXid();
    }

    @Override
    protected int getPayloadSize() {
        return Long.BYTES*2+Integer.BYTES+Short.BYTES+filePath.length();
    }

    @Override
    protected int serializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset)
                .putLong(xid)
                .putLong(prevLsn)
                .putInt(fid)
                .putShort((short)filePath.length())
                .put(filePath.getBytes());
        return buffer.position();
    }



    @Override
    public LogType getType() {
        return LogType.CREATE_FILE;
    }
}
