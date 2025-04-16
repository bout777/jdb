package com.jdb.recovery.logs;

import com.jdb.recovery.LogRecord;
import com.jdb.recovery.LogType;
import com.jdb.transaction.TransactionManager;

import java.nio.ByteBuffer;

public class CommitLog extends LogRecord {
    long xid;

    public CommitLog(long xid) {
        super(LogType.COMMIT);
        this.xid = xid;
    }

    public static LogRecord deserializePayload(ByteBuffer buffer, int offset) {
        long xid = buffer.getLong(offset);
        return new CommitLog(xid);
    }



    @Override
    public long getXid() {
        return xid;
    }


    @Override
    protected int getPayloadSize() {
        return Long.BYTES ;
    }

    @Override
    protected int serializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset)
                .putLong(xid);
        return buffer.position();
    }

    @Override
    public LogType getType() {
        return LogType.COMMIT;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommitLog that = (CommitLog) o;
        return xid == that.xid;
    }

    @Override
    public String toString() {
        return "CommitLog{" +
                "xid=" + xid +
                '}';
    }
}
