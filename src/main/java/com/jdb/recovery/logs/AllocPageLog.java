package com.jdb.recovery.logs;

import com.jdb.recovery.LogType;

import java.nio.ByteBuffer;

public class AllocPageLog extends LogRecord{
    long xid;
    long prevLsn;
    long pid;
    protected AllocPageLog() {
        super(LogType.ALLOC_PAGE);
    }

    @Override
    protected int getPayloadSize() {
        return 0;
    }

    @Override
    protected int serializePayload(ByteBuffer buffer, int offset) {
        return 0;
    }

    @Override
    public LogType getType() {
        return LogType.ALLOC_PAGE;
    }
}
