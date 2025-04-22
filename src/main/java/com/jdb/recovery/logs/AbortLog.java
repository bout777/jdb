package com.jdb.recovery.logs;

import com.jdb.recovery.LogType;

import java.nio.ByteBuffer;

public class AbortLog extends LogRecord {
    long xid;
    long prevLsn;
    public AbortLog(long xid,long prevLsn) {
        super(LogType.ABORT);
        this.prevLsn = prevLsn;
    }

    @Override
    protected int getPayloadSize() {
        return Long.BYTES * 2;
    }

    @Override
    protected int serializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset)
                .putLong(xid)
                .putLong(prevLsn);
        return buffer.position();
    }

    @Override
    public LogType getType() {
        return LogType.ABORT;
    }

    public static LogRecord deserializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset);
        long xid = buffer.getLong();
        long prevLsn = buffer.getLong();
        return new AbortLog(xid,prevLsn);
    }

    @Override
    public String toString() {
        return "AbortLog{" +
                "xid=" + xid +
                ", prevLsn=" + prevLsn +
                '}';
    }
}
