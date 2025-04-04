package com.jdb.table;

public class RecordID {
    public static final int SIZE = Integer.BYTES * 2;
    public int pageId;
    public int slotId;

    public RecordID(int pageId, int slotId) {
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
