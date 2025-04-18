package com.jdb.storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.jdb.common.Constants.PAGE_SIZE;

public class JBFile {
  private   ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private RandomAccessFile file;
    private int fid;
    Lock rlock = rwLock.readLock();
    Lock wlock = rwLock.writeLock();


    JBFile(String fileName) {
        try {
            this.file = new RandomAccessFile(fileName, "rwd");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void read(int pno, byte[] data) throws IOException {
        int pos = pno * PAGE_SIZE;
        if (pos > file.length()) {
            throw new NoSuchElementException("pno out of range");
        }
        file.seek(pos);
        file.read(data);
    }

    public void write(int pno, byte[] data) throws IOException {
        int pos = pno * PAGE_SIZE;
        file.seek(pos);
        file.write(data);
    }

}
