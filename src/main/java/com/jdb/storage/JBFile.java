package com.jdb.storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.jdb.common.Constants.PAGE_SIZE;

public class JBFile implements AutoCloseable {
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    Lock rlock = rwLock.readLock();
    Lock wlock = rwLock.writeLock();
    private RandomAccessFile file;
    private int fid;
    private String name;

    JBFile(String fileName) {
        try {
            this.file = new RandomAccessFile(fileName, "rwd");
            this.name = fileName;
//            System.out.println("file"+name+" op");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void read(int pno, byte[] data) throws IOException {
        int pos = pno * PAGE_SIZE;
        if (pos >= file.length()) {
            throw new NoSuchElementException("pno out of range");
        }
        file.seek(pos);
        file.read(data);
    }

    public void write(int pno, byte[] data) throws IOException {
        int pos = pno * PAGE_SIZE;
//        System.out.println(+"write: "+);
//        System.out.println(name+" write: "+pos);
        file.seek(pos);
        file.write(data);
    }

    public long length() {
        try {
            return file.length();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void close() throws Exception {
//        System.out.println("file"+name+" close");
        file.close();
    }
}
