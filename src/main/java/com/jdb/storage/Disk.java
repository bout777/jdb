package com.jdb.storage;

import java.io.IOException;
import java.io.RandomAccessFile;

import static com.jdb.common.Constants.PAGE_SIZE;

public class Disk {
    RandomAccessFile file;

    //页面在磁盘中以pageId连续存储
    public void readPage(String path, int pageId, byte[] data)  {
        try {
            file = new RandomAccessFile(path, "r");
            int pos = pageId * PAGE_SIZE;
            if(pos > file.length())
                throw new RuntimeException("pageId out of range");
            file.seek((long) pageId * PAGE_SIZE);
            file.read(data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void writePage(String path, int pageId, byte[] data) {
        try {
            file = new RandomAccessFile(path, "rw");
            file.seek((long) pageId * PAGE_SIZE);
            file.write(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
