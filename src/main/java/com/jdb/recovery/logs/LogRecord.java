package com.jdb.recovery.logs;

import com.jdb.recovery.LogType;

import java.nio.ByteBuffer;

import static com.jdb.common.Constants.NULL_LSN;
import static com.jdb.common.Constants.NULL_PAGE_ID;

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
            case UPDATE -> UpdateLog.deserializePayload(buffer, offset);
            case MASTER -> MasterLog.deserializePayload(buffer, offset);
            case INSERT -> InsertLog.deserializePayload(buffer, offset);
            case CHECKPOINT -> CheckpointLog.deserializePayload(buffer, offset);
            case BEGIN -> BeginLog.deserializePayload(buffer, offset);
            case COMMIT -> CommitLog.deserializePayload(buffer, offset);
            case ABORT -> AbortLog.deserializePayload(buffer, offset);
            case DELETE -> DeleteLog.deserializePayload(buffer, offset);
            case COMPENSATION -> CompensationLog.deserializePayload(buffer, offset);
            default -> throw new UnsupportedOperationException("bad log type");
        };
    }

    public long getPrevLsn(){return NULL_LSN;} ;

    public  long getPageId(){return NULL_PAGE_ID;}

    public long getLsn(){return lsn;}

    public void setLsn(long lsn){this.lsn = lsn;}

    public long getXid(){return -1L;}

//    public void setXid(long xid){};

    protected abstract int getPayloadSize();

    protected abstract int serializePayload(ByteBuffer buffer, int offset);
    
    protected int serializeHeader(ByteBuffer buffer, int offset) {
        buffer.put(offset, (byte) type.getValue());
        offset += Byte.BYTES;
        return offset;
    }

    public int getSize(){
        return HEADER_SIZE + getPayloadSize();
    };

    public int serialize(ByteBuffer buffer, int offset) {
        offset = serializeHeader(buffer, offset);
        offset = serializePayload(buffer, offset);
        return offset;
    }


    // 为了测试写成默认方法，后续要改成子类重载
    public byte[] toBytes() {
        return new byte[0];
    }

    public abstract LogType getType();

    public void redo(){
    };
    public void undo(){};
}
