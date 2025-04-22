package com.jdb;

import com.jdb.storage.Disk;
import com.jdb.storage.Page;

import java.util.HashMap;
import java.util.Map;

import static com.jdb.common.Constants.PAGE_SIZE;

public class MockDisk extends Disk {
    //模拟磁盘中的页，供bufferPool存取
    Map<Long ,Page> diskPages = new HashMap<>();
    public MockDisk(String path) {
        super(path);
    }

    @Override
    public Page readPage(long pid) {
        Page page = diskPages.get(pid);
        if(page==null){
            throw new NullPointerException("page not found");
        }
        return page;
    }
//
    @Override
    public void writePage(Page page) {
        Page pageInDisk = new Page(page.pid);
        System.arraycopy(page.getData(), 0, pageInDisk.getData(), 0, PAGE_SIZE);
        diskPages.put(page.pid, pageInDisk);
    }
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
