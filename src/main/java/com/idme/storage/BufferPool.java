package com.idme.storage;


import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BufferPool {
    private static BufferPool instance;
    /*
     * 为了实现单表暂时这样写
     * 后续要根据表空间来分配
     * 缓冲池内应该有多张表的页*/
    private static int maxPageId = 0;
    private final Disk disk;
    private final HashMap<Integer, Page> buffers;

    private BufferPool(Disk disk) {
        buffers = new HashMap<Integer, Page>();
        this.disk = disk;
    }

    public static BufferPool getInstance() {
        if (instance == null) {
            instance = new BufferPool(new Disk());
            return instance;
        }
        return instance;
    }

    public int getMaxPageId() {
        return maxPageId;
    }

    public Page getPage(int pageId) {
        Page page = buffers.get(pageId);
        if (page == null) {
            page = new Page();
            disk.readPage("test.db", pageId, page.getData());
//            page.deserialize();
        } else {
            return page;
        }
        return page;
    }

    public Page newPage(int pageId) {
        Page page = new Page();
        buffers.put(pageId, page);
        maxPageId = pageId + 1;
        return page;
    }

    public void flush() {
        Set<Map.Entry<Integer, Page>> entries = buffers.entrySet();
        for (Map.Entry<Integer, Page> entry : entries) {
            Page page = entry.getValue();
            if (page.isDirty()) {
                disk.writePage("test.db", entry.getKey(), page.getData());
            }
        }
    }

    void flushPage(int pageId) {
        Page page = buffers.get(pageId);
        if (page == null)
            throw new RuntimeException("page not found");
        disk.writePage("test.db", pageId, page.getData());
    }


}
