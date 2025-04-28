package com.jdb.storage;


import com.jdb.Engine;
import com.jdb.recovery.RecoveryManager;
import com.jdb.table.DataPage;
import com.jdb.transaction.TransactionContext;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class BufferPool {
    Engine engine;
    //    private static volatile BufferPool instance;
    /*
     * 为了实现单表暂时这样写
     * 后续要根据表空间来分配
     * 缓冲池内应该有多张表的页*/
    private Disk disk;
    private final Map<Long, Page> buffers = new ConcurrentHashMap<>();
    private RecoveryManager recoveryManager;


    public BufferPool(Disk disk) {
        this.disk = disk;
    }

    public BufferPool(Engine engine) {
        this.engine = engine;
    }

    public void injectDependency() {
        disk = engine.getDisk();
        recoveryManager = engine.getRecoveryManager();
    }

    public Disk getDisk() {
        return disk;
    }

//    public synchronized static BufferPool getInstance() {
//        if (instance == null) {
//            instance = new BufferPool(Disk.getInstance());
//        }
//        return instance;
//    }


    public Page getPage(long pid) {
        Page page = buffers.get(pid);
        if (page == null) {
            page = disk.readPage(pid);
            buffers.put(pid, page);
        }
        return page;
    }


    public Page newPage(int fid, boolean shouldLog) {
        Page page = disk.allocPage(fid);
        buffers.put(page.pid, page);
        if (shouldLog) {
            long xid = TransactionContext.getTransaction().getXid();
            long lsn= recoveryManager.logPageAlloc(xid, page.pid);
            page.setLsn(lsn);
        }
        return page;
    }

    public void flush() {
        Set<Map.Entry<Long, Page>> entries = buffers.entrySet();
        for (Map.Entry<Long, Page> entry : entries) {
            Page page = entry.getValue();
            page.acquireWriteLock();
            try {
                disk.writePage(page);
                page.setDirty(false);
            } finally {
                page.releaseWriteLock();
            }
        }
    }


    public void flushPage(long pid) {

        Page page = buffers.get(pid);
        if (page == null)
            throw new RuntimeException("page not found");
        page.acquireWriteLock();
        try {
            disk.writePage(page);
            page.setDirty(false);
        } finally {
            page.releaseWriteLock();
        }
    }

    public void close() {
        buffers.clear();
    }

}
