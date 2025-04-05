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
    private static int maxPageId = 0;
    private final Disk disk;
    private final Map<Integer, Page> buffers;
    // 临时修改，为了配合日志文件的测试
    private final Map<String, Page> buf = new HashMap<>();

    // (filename->nextPageId)
    private final Map<String, Integer> nextPage = new HashMap<>();

    private BufferPool(Disk disk) {
        buffers = new ConcurrentHashMap<>();
        this.disk = disk;
    }

    public static BufferPool getInstance() {
        if (instance == null) {
            synchronized (BufferPool.class) {
                if (instance == null) {
                    instance = new BufferPool(new Disk());
                }
            }
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
        } else {
            return page;
        }
        buffers.put(pageId, page);
        return page;
    }

    public Page getPage(String fileName, int pageId) {
        String key = String.format("%s/%d", fileName, pageId);
        Page page = buf.get(key);
        if (page == null) {
            page = new Page();
            disk.readPage(fileName, pageId, page.getData());
            buf.put(key, page);
        }
        return page;
    }

    public Page newPage(String fileName) {
        Page page = new Page();
        page.pid = nextPage.getOrDefault(fileName, 0);
        nextPage.put(fileName, page.pid + 1);
        buffers.put(page.pid, page);
        return page;
    }

    public Page newPage(String fileName, int pageId) {
        Page page = new Page();
        String key = String.format("%s/%d", fileName, pageId);
        buf.put(key, page);
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
        page.setDirty(false);
    }

    public void flushPage(String fileName, int pageId) {
        Page page = buf.get(fileName + pageId);
        if (page == null)
            throw new RuntimeException("page not found");
        disk.writePage(fileName, pageId, page.getData());
        page.setDirty(false);
    }

    public void shutdown() {
        buffers.clear();
    }

}
