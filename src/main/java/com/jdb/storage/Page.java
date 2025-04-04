package com.jdb.storage;

import java.nio.ByteBuffer;

import static com.jdb.common.Constants.PAGE_SIZE;

public class Page {
    private byte[] data;
    private boolean isDirty;
    private ByteBuffer buffer;
    public Page() {
        this.data = new byte[PAGE_SIZE];
        this.buffer = ByteBuffer.wrap(this.data);
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

    public ByteBuffer getBuffer() {
        return this.buffer;
    }

}
