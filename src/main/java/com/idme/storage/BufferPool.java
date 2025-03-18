package com.idme.storage;

import com.idme.table.Page;

import java.util.HashMap;

public class BufferPool {
    private static BufferPool instance;
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

    public Page getPage(int pageId) {
        Page page = buffers.get(pageId);
        if (page == null) {
            page = new Page(pageId);
            disk.readPage("test.db", pageId, page.getData());
            page.deserialize();
        } else {
            return page;
        }
        return page;
    }

    public Page newPage(int pageId) {
        Page page = new Page(pageId);
        buffers.put(pageId, page);
        return page;
    }

    public void flush() {
        for (Page page : buffers.values()) {
            if (page.isDirty()) {
                page.serialize();
                disk.writePage("test.db", page.pageId, page.getData());
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
