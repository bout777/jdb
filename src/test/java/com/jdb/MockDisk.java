package com.jdb;

import com.jdb.storage.Disk;
import com.jdb.storage.Page;

public class MockDisk extends Disk {
    public MockDisk(String path) {
        super(path);
    }

//    @Override
//    public Page readPage(long pid) {
//
//    }
//
//    @Override
//    public void writePage(Page page) {
//
//    }
//
//    @Override
//    public Page allocPage(int fid) {
//
//    }
//
//    @Override
//    public void putFile(int fid, String fileName) {
//
//    }
}
