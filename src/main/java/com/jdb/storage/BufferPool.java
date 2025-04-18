package com.jdb.storage;


import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class BufferPool {
    private static volatile BufferPool instance;
    /*
     * 为了实现单表暂时这样写
     * 后续要根据表空间来分配
     * 缓冲池内应该有多张表的页*/
    private final Disk disk;
    private final Map<Long, Page> buffers;


    public BufferPool(Disk disk) {
        buffers = new ConcurrentHashMap<>();
        this.disk = disk;
    }

    public Disk getDisk(){
        return disk;
    }

    public synchronized static BufferPool getInstance() {
        if (instance == null) {
            instance = new BufferPool(Disk.getInstance());
        }
        return instance;
    }


    public Page getPage(long pid) {
        Page page = buffers.get(pid);
        if (page == null) {
            throw new RuntimeException("page not found");
//            page = disk.readPage(pid);
        } else {
//            return page;
        }
        buffers.put(pid, page);
        return page;
    }


    public Page newPage(String fileName) {
        Page page = new Page();
        page.pid = disk.getNextPageIdAndIncrease(fileName);
        buffers.put(page.pid, page);
        return page;
    }


    public Page newPage(int fid) {
        Page page = new Page();
        page.pid = disk.getNextPageIdAndIncrease(fid);
        buffers.put(page.pid, page);
        return page;
    }

    public void flush() {
        Set<Map.Entry<Long, Page>> entries = buffers.entrySet();
        for (Map.Entry<Long, Page> entry : entries) {
            Page page = entry.getValue();
            int pno = getPno(entry.getKey());
            if (page.isDirty()) {
                disk.writePage("test.db", pno, page.getData());
            }
        }
    }

    private int getFid(long pid) {
        return (int) (pid >> 32);
    }

    private int getPno(long pid) {
        return (int) (pid & 0xffffffffL);
    }

    public void flushPage(long pid) {
        int fid = getFid(pid);
        int pno = getPno(pid);
        Page page = buffers.get(pid);
        if (page == null)
            throw new RuntimeException("page not found");
        disk.writePage(fid, pno, page.getData());
        page.setDirty(false);
    }

    public void shutdown() {
        buffers.clear();
    }

}
