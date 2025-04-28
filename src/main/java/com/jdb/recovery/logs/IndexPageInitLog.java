package com.jdb.recovery.logs;

import com.jdb.Engine;
import com.jdb.recovery.LogType;

import java.nio.ByteBuffer;

public class IndexPageInitLog  extends LogRecord{

    protected IndexPageInitLog() {
        super(LogType.INDEX_PAGE_INIT);
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
        return LogType.INDEX_PAGE_INIT;
    }

    @Override
    public void redo(Engine engine) {

    }
    @Override
    public void undo(Engine engine) {

    }
}
