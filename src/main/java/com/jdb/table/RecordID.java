package com.jdb.table;

import java.nio.ByteBuffer;

public class RecordID {
    public static final int SIZE = Long.BYTES+Integer.BYTES;
    public long pid;
    public int slotId;

    public RecordID(long pid, int slotId) {
        this.pid = pid;
        this.slotId = slotId;
    }

    @Override
    public String toString() {
        return "RecordID{" +
                "pageId=" + pid +
                ", slotId=" + slotId +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof RecordID that)
            return this.pid == that.pid && this.slotId == that.slotId;
        return false;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(pid);
        result = 31 * result + Integer.hashCode(slotId);
        return result;
    }

    public static RecordID deserialize(ByteBuffer buffer, int offset) {
        buffer.position(offset);
        long pid = buffer.getLong();
        int slotId = buffer.getInt();
        return new RecordID(pid, slotId);
    }

}
