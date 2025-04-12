package com.jdb.table;

import java.nio.ByteBuffer;

public class RecordID {
    public static final int SIZE = Long.BYTES+Integer.BYTES;
    public long pid;
    public int offset;

    public RecordID(long pid, int offset) {
        this.pid = pid;
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "RecordID{" +
                "pageId=" + pid +
                ", offset=" + offset +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof RecordID that)
            return this.pid == that.pid && this.offset == that.offset;
        return false;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(pid);
        result = 31 * result + Integer.hashCode(offset);
        return result;
    }

    public static RecordID deserialize(ByteBuffer buffer, int offset) {
        buffer.position(offset);
        long pid = buffer.getLong();
        int slotId = buffer.getInt();
        return new RecordID(pid, slotId);
    }

}
