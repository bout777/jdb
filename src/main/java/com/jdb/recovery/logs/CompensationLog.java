package com.jdb.recovery.logs;

import com.jdb.recovery.LogRecord;
import com.jdb.recovery.LogType;

import java.nio.ByteBuffer;

/**
 * clr中有两个数据<br/>
 * undoNextlsn用于undo阶段<br/>
 * redolsn用于redo阶段<br/>
 * 在redo阶段中，clr的redo方法会被调用<br/>
 * 根据redolsn取出log，执行log的undo操作->历史记录重做<br/>
 * <p>
 * 在undo阶段，只需要将clr的undoNextLsn取出来即可
 */
public class CompensationLog extends LogRecord {
    int undoNextLsn;

    protected CompensationLog(LogType type) {
        super(type);
    }

    public static LogRecord deserialize(ByteBuffer buffer, int offset) {
        return null;
    }

    public int getUndoNextLsn() {
        return undoNextLsn;
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
        return LogType.COMPENSATION;
    }

    @Override
    public void redo() {

    }

    @Override
    public void undo() {

    }
}
