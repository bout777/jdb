package com.jdb.recovery.logs;

import com.jdb.common.DataType;
import com.jdb.common.Value;
import com.jdb.recovery.LogType;

import java.nio.ByteBuffer;

public class IndexEntryInsertLog extends LogRecord {
    long xid;
    long prevLsn;
    long pid;

    long entryPid;
    Value<?> key;

    public IndexEntryInsertLog(long xid, long prevLsn, long pid, Value<?> key, long entryPid) {
        super(LogType.INDEX_PAGE_INSERT);

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
    public long getPrevLsn() {
        return prevLsn;
    }

    @Override
    protected int getPayloadSize() {
        return Long.BYTES * 4 + Byte.BYTES + key.getBytes();
    }

    protected int serializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset)
                .putLong(xid)
                .putLong(prevLsn)
                .putLong(pid)
                .putLong(entryPid)
                .put((byte) key.getType().ordinal());
        return key.serialize(buffer, offset + Long.BYTES * 4);
    }

    public static LogRecord deserializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset);
        long xid = buffer.getLong();
        long prevLsn = buffer.getLong();
        long pid = buffer.getLong();
        long entryPid = buffer.getLong();
        DataType type = DataType.values()[buffer.get()];
        offset = buffer.position();
        Value<?> key = Value.deserialize(buffer, offset, type);
        return new IndexEntryInsertLog(xid, prevLsn, pid, key, entryPid);
    }

    @Override
    public LogType getType() {
        return LogType.INDEX_PAGE_INSERT;
    }
}
