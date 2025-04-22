package com.jdb;

import com.jdb.common.PageHelper;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Disk;
import com.jdb.storage.Page;

import java.util.HashMap;
import java.util.Map;

public class DummyBufferPool extends BufferPool {
    Map<Long, Page> pages = new HashMap<>();
    Map<Integer, Long> nextPages = new HashMap<>();
    private Disk disk ;
    @Override
    public Page getPage(long pid) {
        Page page = pages.get(pid);
        if(page==null){
            page = disk.readPage(pid);
            pages.put(pid, page);
        }
        return page;
    }

    @Override
    public Page newPage(int fid) {
        long pid = nextPages.getOrDefault(fid, PageHelper.concatPid(fid, 0));
        Page page = new Page(pid);
        pages.put(pid, page);
        nextPages.put(fid, pid + 1);
        return page;
    }

    @Override
    public void flushPage(long pid) {
        disk.writePage(pages.get(pid));
    }

    @Override
    public void shutdown() {
        pages.clear();
    }

    public DummyBufferPool() {
        super(new MockDisk("aldksj"));
        disk = getDisk();
    }
}
