package com.jdb.recovery.logs;

import com.jdb.Engine;
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
    LogRecord originLog;

    public CompensationLog(LogRecord originLog) {
        super(LogType.COMPENSATION);
        this.originLog = originLog;
    }

    public static LogRecord deserializePayload(ByteBuffer buffer, int offset) {
        LogRecord originLog = LogRecord.deserialize(buffer, offset);
        return new CompensationLog(originLog);
    }

    public long getUndoNextLsn() {
        return originLog.getPrevLsn();
    }


    @Override
    public long getXid() {
        return originLog.getXid();
    }


    @Override
    protected int getPayloadSize() {
        return originLog.getSize();
    }

    @Override
    protected int serializePayload(ByteBuffer buffer, int offset) {
        return originLog.serialize(buffer, offset);
    }

    @Override
    public LogType getType() {
        return LogType.COMPENSATION;
    }

    @Override
    public void redo(Engine engine) {
//        originLog.undo(com.jdb.engine);
    }

    @Override
    public void undo(Engine engine) {
        throw new UnsupportedOperationException("CompensationLog can not be undo");
    }

    @Override
    public String toString() {
        return "CompensationLog{" +
                "origin=" + originLog +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof CompensationLog that)
            return this.originLog.equals(that.originLog);
        return false;
    }
}
