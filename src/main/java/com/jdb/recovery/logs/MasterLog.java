package com.jdb.recovery.logs;

import com.jdb.recovery.LogRecord;
import com.jdb.recovery.LogType;

import java.nio.ByteBuffer;

public class MasterLog extends LogRecord {
    private long lastCheckpointLsn;

    public MasterLog(long lastCheckpointLsn) {
        super(LogType.MASTER);
        this.lastCheckpointLsn = lastCheckpointLsn;
    }

    public static LogRecord deserialize(ByteBuffer buffer, int offset) {
        long lastCheckpointLsn = buffer.getLong(offset);
        return new MasterLog(lastCheckpointLsn);
    }

    @Override
    public int getPageId() {
        return 0;
    }

    @Override
    public long getLsn() {
        return 0;
    }

    @Override
    public void setLsn(long lsn) {

    }

    @Override
    public long getXid() {
        return 0;
    }

    @Override
    public void setXid(long xid) {

    }

    @Override
    public int getSize() {
        return 0;
    }

    @Override
    public int serialize(ByteBuffer buffer, int offset) {
        buffer.put(offset, (byte) getType().getValue());
        offset += Byte.BYTES;
        buffer.putLong(offset, lastCheckpointLsn);
        return offset + Long.BYTES;
    }

    @Override
    public LogType getType() {
        return LogType.MASTER;
    }

    @Override
    public void redo() {

    }

    @Override
    public void undo() {

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MasterLog masterLog = (MasterLog) o;
        return lastCheckpointLsn == masterLog.lastCheckpointLsn;
    }
}
