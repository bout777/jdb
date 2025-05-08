package com.jdb.recovery.logs;

import com.jdb.Engine;
import com.jdb.recovery.LogType;

import java.nio.ByteBuffer;

public class MasterLog extends LogRecord {
    private final long lastCheckpointLsn;

    public MasterLog(long lastCheckpointLsn) {
        super(LogType.MASTER);
        this.lastCheckpointLsn = lastCheckpointLsn;
    }

    public static LogRecord deserializePayload(ByteBuffer buffer, int offset) {
        long lastCheckpointLsn = buffer.getLong(offset);
        return new MasterLog(lastCheckpointLsn);
    }

    public long getLastCheckpointLsn() {
        return lastCheckpointLsn;
    }

    @Override
    public long getPageId() {
        return 0;
    }

    @Override
    public long getLsn() {
        return 0;
    }


    @Override
    public long getXid() {
        return 0;
    }


    @Override
    protected int getPayloadSize() {
        return 0;
    }

    @Override
    protected int serializePayload(ByteBuffer buffer, int offset) {
        buffer.putLong(offset, lastCheckpointLsn);
        return offset + Long.BYTES;
    }

    @Override
    public LogType getType() {
        return LogType.MASTER;
    }

    @Override
    public void redo(Engine engine) {

    }

    @Override
    public void undo(Engine engine) {

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MasterLog masterLog = (MasterLog) o;
        return lastCheckpointLsn == masterLog.lastCheckpointLsn;
    }

    @Override
    public String toString() {
        return "MasterLog{" +
                "lastCheckpointLsn=" + lastCheckpointLsn +
                '}';
    }
}
