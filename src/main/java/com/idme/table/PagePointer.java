package com.idme.table;

public class PagePointer {
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
