package com.jdb.storage;

import static com.jdb.common.Constants.PAGE_SIZE;

public class Page {
    private byte[] data;
    private boolean isDirty;

    public Page() {
        this.data = new byte[PAGE_SIZE];
    }

    public boolean isDirty() {
        return this.isDirty;
    }

    public void setDirty(boolean dirty) {
        this.isDirty = dirty;
    }

    public byte[] getData() {
        return this.data;
    }


}
