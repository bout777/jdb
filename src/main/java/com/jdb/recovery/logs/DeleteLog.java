package com.jdb.recovery.logs;

import com.jdb.recovery.LogRecord;
import com.jdb.recovery.LogType;

import java.nio.ByteBuffer;

import static com.jdb.common.Constants.NULL_PAGE_ID;

public class DeleteLog extends LogRecord {
    protected DeleteLog() {
        super(LogType.DELETE);
    }

    public static LogRecord deserialize(ByteBuffer buffer, int offset) {
        return null;
    }

    @Override
    public int getPageId() {
        return NULL_PAGE_ID;
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
