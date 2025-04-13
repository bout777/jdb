package com.jdb.version;

import com.jdb.table.Record;
import com.jdb.table.PagePointer;
import com.jdb.transaction.TransactionContext;
import com.jdb.transaction.TransactionManager;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/*
 * 多版本管理实现
 * 读写整合到datapage里，反正日志模块已经干了，不怕再加了
 * datapage读写调用vm，至于锁，再看看
 *
 *
 *
 *
 * */
public class VersionManager {

    private static VersionManager instance = new VersionManager();

    public static VersionManager getInstance() {
        return instance;
    }

    private ReadWriteLock rw = new ReentrantReadWriteLock();
    private Map<LogicRid, VersionEntrySet> versionMap = new HashMap<>();

    private class LogicRid {
        private String tableName;
        private int primaryKey;
        private LogicRid(String tableName, int primaryKey) {
            this.tableName = tableName;
            this.primaryKey = primaryKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o instanceof LogicRid that)
                return this.tableName.equals(that.tableName) && this.primaryKey == that.primaryKey;
            return false;
        }
        @Override
        public int hashCode() {
            return Objects.hash(tableName, primaryKey);
        }
    }
    //fixme 当页分裂后，原来的rid可能不再对应原来的记录，需要修复
    public void pushUpdate(String tableName, Record record) {
        var key = new LogicRid(tableName, record.getPrimaryKey());

        var deque = versionMap.get(key);
        if (deque == null) {
            deque = new VersionEntrySet(key);
            versionMap.put(key, deque);
        }
        if (!deque.tryPush(record)) {
            //todo 冲突回滚
            throw new RuntimeException("测试代码不该跑到这,tell me why?");
        }
    }

    public Record read(PagePointer ptr) {
        rw.readLock().lock();

        var entrySet = versionMap.get(ptr);
        if (entrySet == null)
            return null;

        var isolevel = TransactionContext.getTransaction().getIsolationLevel();
        Record record = null;
        switch (isolevel) {
            case READ_COMMITTED -> {
                long curTs = TransactionManager.getCurrentTrxStamp();
                var searchKey = new VersionEntry(curTs, null);
                record = entrySet.ceiling(searchKey).getRecord();
            }
            case SNAPSHOT_ISOLATION -> {
                long xid = TransactionContext.getTransaction().getXid();
                var searchKey = new VersionEntry(xid, null);
                record = entrySet.floor(searchKey).getRecord();
            }
        }

        rw.readLock().unlock();

        return record;
    }

    public void commit(Set<PagePointer> writeSet) {
        long curTs = TransactionManager.getCurrentTrxStamp();
        //事务提交期间，阻塞读线程
        rw.writeLock().lock();

        //这里用了O(klogn),后续可以优化成O(k)？
        for (PagePointer ptr : writeSet) {
            var deque = versionMap.get(ptr);
            for (VersionEntry entry : deque) {
                if (entry.getStartTs() == curTs) {
                    entry.setEndTs(TransactionManager.getCurrentTrxStamp());
                }
            }
        }

        rw.writeLock().unlock();
    }



    //maintain a version list for each record
    private class VersionEntrySet extends TreeSet<VersionEntry> {
        LogicRid ptr;
        VersionEntrySet(LogicRid ptr) {
            super(Comparator.comparingLong(VersionEntry::getEndTs));
            this.ptr = ptr;
        }

        public synchronized boolean tryPush(Record record) {
            long xid = TransactionContext.getTransaction().getXid();
            VersionEntry entry = new VersionEntry(xid, record);

            if (this.isEmpty()) {
                this.add(entry);
                return true;
            }

            var head = this.last();
            if (head.endTs == Long.MAX_VALUE) {
                return false;
            }

            this.add(entry);
            return true;
        }


//        private boolean tryPushInReadCommitted(VersionEntry entry){
//            long curTs = TransactionManager.getCurrentXid();
//            VersionEntry head = peek();
//            if(head.endTs>curTs)
//                return false;
//
//            this.addFirst(entry);
//            return true;
//        }
//
//        private boolean tryPushInSnapshotIsolation(VersionEntry entry){
//            VersionEntry head = peek();
//            if(head.endTs>entry.startTs)
//                return false;
//
//            this.addFirst(entry);
//            return true;
//        }
    }
}
