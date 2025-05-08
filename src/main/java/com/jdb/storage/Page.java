package com.jdb.storage;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.jdb.common.Constants.PAGE_SIZE;

public class Page {
    // 这个字段是用来给page的包装类提供一个缓冲池分配的页id
    public long pid;
    private final byte[] data;
    private boolean isDirty;
    private final ByteBuffer buffer;
    private final ReadWriteLock rw = new ReentrantReadWriteLock();

    public Page(long pid) {
        this.data = new byte[PAGE_SIZE];
        this.buffer = ByteBuffer.wrap(this.data);
        this.pid = pid;
    }

    public Page(byte[] data) {
        this.data = data;
        this.buffer = ByteBuffer.wrap(this.data);
    }

    public long getLsn() {
        return buffer.getLong(Byte.BYTES);
    }

    public void setLsn(long lsn) {
        buffer.putLong(Byte.BYTES, lsn);
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

    public void acquireReadLock() {
        this.rw.readLock().lock();
    }

    public void releaseReadLock() {
        this.rw.readLock().unlock();
    }

    public void acquireWriteLock() {
        this.rw.writeLock().lock();
    }

    public void releaseWriteLock() {
        this.rw.writeLock().unlock();
    }
}
