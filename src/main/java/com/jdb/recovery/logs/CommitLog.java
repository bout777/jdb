package com.jdb.recovery.logs;

import com.jdb.recovery.LogRecord;
import com.jdb.recovery.LogType;

import java.nio.ByteBuffer;

public class CommitLog extends LogRecord {

    public CommitLog(long xid) {
        super(LogType.COMMIT);
    }

    public static LogRecord deserialize(ByteBuffer buffer, int offset) {
        return null;
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
    public int getSize() {
        return 0;
    }

    @Override
    public int serialize(ByteBuffer buffer, int offset) {
        return 0;
    }

    @Override
    public LogType getType() {
        return null;
    }

    @Override
    public void redo() {

    }

    @Override
    public void undo() {

    }
}
