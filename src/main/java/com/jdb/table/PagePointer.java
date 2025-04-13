package com.jdb.table;

import java.nio.ByteBuffer;

/**
 * 页的物理指针，指向确定页的确定位置
 */
public class PagePointer {
    public static final int SIZE = Long.BYTES+Integer.BYTES;
    public long pid;
    public int offset;

    public PagePointer(long pid, int offset) {
        this.pid = pid;
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "PagePointer{" +
                "pageId=" + pid +
                ", offset=" + offset +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof PagePointer that)
            return this.pid == that.pid && this.offset == that.offset;
        return false;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(pid);
        result = 31 * result + Integer.hashCode(offset);
        return result;
    }

    public static PagePointer deserialize(ByteBuffer buffer, int offset) {
        buffer.position(offset);
        long pid = buffer.getLong();
        int slotId = buffer.getInt();
        return new PagePointer(pid, slotId);
    }

}
