package com.jdb.recovery.logs;

import com.jdb.recovery.LogRecord;
import com.jdb.recovery.LogType;

import java.nio.ByteBuffer;

import static com.jdb.common.Constants.NULL_PAGE_ID;

public class BeginLog extends LogRecord {

    public BeginLog(long xid) {
        super(LogType.BEGIN);
        this.setXid(xid);
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
        return 0;
    }

    @Override
    public LogType getType() {
        return LogType.BEGIN;
    }

    @Override
    public void redo() {

    }

    @Override
    public void undo() {

    }
}
