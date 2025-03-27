package com.jdb.table;

public class PagePointer {
    public static final int SIZE = Integer.BYTES * 2;
    public int pageId;
    public int slotId;

    public PagePointer(int pageId, int slotId) {
        this.pageId = pageId;
        this.slotId = slotId;
    }

    @Override
    public String toString() {
        return "PagePointer{" +
                "pageId=" + pageId +
                ", slotId=" + slotId +
                '}';
    }
}
