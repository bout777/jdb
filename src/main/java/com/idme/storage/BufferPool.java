package com.idme.storage;


import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
        return page;
    }

    public void flush() {
//        for (Page page : buffers.values()) {
//            if (page.isDirty()) {
////                page.serialize();
//                disk.writePage("test.db", page.pageId, page.getData());
//            }
//        }
        Set<Map.Entry<Integer, Page>> entries = buffers.entrySet();
        for (Map.Entry<Integer, Page> entry : entries) {
            Page page = entry.getValue();
//            if (page.isDirty()) {
                disk.writePage("test.db", entry.getKey(), page.getData());
//            }
        }
    }

    void flushPage(int pageId) {
        Page page = buffers.get(pageId);
        if (page == null)
            throw new RuntimeException("page not found");
        disk.writePage("test.db", pageId, page.getData());
    }


}
