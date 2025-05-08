package com.jdb.recovery.logs;

import com.jdb.recovery.LogType;

import java.nio.ByteBuffer;

public class BeginLog extends LogRecord {
    long xid;

    public BeginLog(long xid) {
        super(LogType.BEGIN);
        this.xid = xid;
    }

    public static LogRecord deserializePayload(ByteBuffer buffer, int offset) {
        return new BeginLog(buffer.getLong(offset));
    }


    @Override
    public long getXid() {
        return xid;
    }


    @Override
    protected int getPayloadSize() {
        return Long.BYTES;
    }

    @Override
    protected int serializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset)
                .putLong(xid);
        return buffer.position();
    }

    @Override
    public LogType getType() {
        return LogType.BEGIN;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BeginLog beginLog = (BeginLog) o;
        return xid == beginLog.xid;
    }

    @Override
    public String toString() {
        return "BeginLog{" +
                "xid=" + xid +
                '}';
    }

}
