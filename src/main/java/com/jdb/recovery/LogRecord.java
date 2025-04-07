package com.jdb.recovery;

import com.jdb.recovery.logs.InsertLog;
import com.jdb.recovery.logs.MasterLog;
import com.jdb.recovery.logs.UpdateLog;

import java.nio.ByteBuffer;

/**
 * 日志类
 */
public abstract class LogRecord {
    protected long lsn;
    protected LogType type;
    protected static final int HEADER_SIZE = Byte.BYTES;
    protected LogRecord(LogType type) {
        this.type = type;
    }

    public static LogRecord deserialize(ByteBuffer buffer, int offset) {
        int type = buffer.get(offset);
        if (type == 0) {
            return null;
        }
        offset += Byte.BYTES;
        return switch (LogType.fromInt(type)) {
            case UPDATE -> UpdateLog.deserialize(buffer, offset);
            case MASTER -> MasterLog.deserialize(buffer, offset);
            case INSERT -> InsertLog.deserialize(buffer, offset);
            default -> throw new UnsupportedOperationException("bad log type");
        };

    }

    public long getPrevLsn(){return -1L;} ;

    public abstract int getPageId();

    public long getLsn(){return lsn;};

    public void setLsn(long lsn){this.lsn = lsn;};

    public long getXid(){return -1L;};

//    public void setXid(long xid){};

    public abstract int getSize();

    public abstract int serialize(ByteBuffer buffer, int offset);
    

    // 为了测试写成默认方法，后续要改成子类重载
    public byte[] toBytes() {
        return new byte[0];
    }

    public abstract LogType getType();

    public abstract void redo();

    public abstract void undo();
}
