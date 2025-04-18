package com.jdb.recovery.logs;

import com.jdb.recovery.LogType;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class CheckpointLog extends LogRecord {
    short dptSize;
    short attSize;
    Map<Long, Long> dpt;
    Map<Long, Long> att;

    public CheckpointLog(Map<Long, Long> dpt, Map<Long, Long> att) {
        super(LogType.CHECKPOINT);
        this.dpt = dpt;
        this.att = att;
        this.dptSize = (short) dpt.size();
        this.attSize = (short) att.size();
    }

    public Map<Long, Long> getDirtyPageTable() {
        return dpt;
    }

    public Map<Long, Long> getActiveTransactionTable() {
        return att;
    }

    @Override
    protected int getPayloadSize() {
        return LogRecord.HEADER_SIZE + 2 * Short.BYTES + dptSize * Long.BYTES + attSize * Long.BYTES;
    }

    @Override
    protected int serializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset)
                .putShort(dptSize)
                .putShort(attSize);

        for (var entry : dpt.entrySet()) {
            buffer.putLong(entry.getKey()).putLong(entry.getValue());
        }

        for (var entry : att.entrySet()) {
            buffer.putLong(entry.getKey()).putLong(entry.getValue());
        }
        return buffer.position();
    }

    public static LogRecord deserializePayload(ByteBuffer buffer, int offset) {
        buffer.position(offset);
        short dptSize = buffer.getShort();
        short attSize = buffer.getShort();
        Map<Long, Long> dpt = new HashMap<>();
        for (short i = 0; i < dptSize; i++) {
            dpt.put(buffer.getLong(), buffer.getLong());
        }
        Map<Long, Long> att = new HashMap<>();
        for (short i = 0; i < attSize; i++) {
            att.put(buffer.getLong(), buffer.getLong());
        }
        return new CheckpointLog(dpt, att);
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
