package com.jdb.version;

import com.jdb.table.Record;
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

    private final ReadWriteLock rw = new ReentrantReadWriteLock();
    private Map<LogicRid, VersionEntrySet> versionMap = new HashMap<>();


    //fixme 当页分裂后，原来的rid可能不再对应原来的记录，需要修复
    public void pushUpdate(String tableName, Record record) {
        var rid = new LogicRid(tableName, record.getPrimaryKey());

        var deque = versionMap.get(rid);
        if (deque == null) {
            deque = new VersionEntrySet(rid);
            versionMap.put(rid, deque);
        }
        if (!deque.tryPush(record)) {
            //todo 冲突回滚
            throw new RuntimeException("测试代码不该跑到这,tell me why?");
        }
    }

    public Record read(String tableName, int primaryKey) {
        rw.readLock().lock();
        var rid = new LogicRid(tableName, primaryKey);
        var entrySet = versionMap.get(rid);
        if (entrySet == null)
            return null;

        var isolevel = TransactionContext.getTransaction().getIsolationLevel();
        Record record = null;
        switch (isolevel) {
            case READ_COMMITTED -> {
                long curTs = TransactionManager.getCurrentTrxStamp();
                var entry = entrySet.floor(curTs);
                if(entry!=null)
                    record = entry.getRecord();
            }
            case SNAPSHOT_ISOLATION -> {
                long xid = TransactionContext.getTransaction().getXid();
                var entry = entrySet.floor(xid);
                if(entry!=null)
                    record = entry.getRecord();
            }
        }
        rw.readLock().unlock();
        return record;
    }

    public void commit(Set<LogicRid> writeSet) {
        long curTs = TransactionManager.getCurrentTrxStamp();
        //事务提交期间，阻塞读线程
        rw.writeLock().lock();

        //这里用了O(klogn),后续可以优化成O(k)？
//        for (PagePointer ptr : writeSet) {
//            var deque = versionMap.get(ptr);
//            for (VersionEntry entry : deque) {
//                if (entry.getStartTs() == curTs) {
//                    entry.setEndTs(TransactionManager.getCurrentTrxStamp());
//                }
//            }
//        }
        for (LogicRid ptr : writeSet) {
            var entries = versionMap.get(ptr);
            var entry = entries.pollLastEntry().getValue();
            entry.setEndTs(curTs);
            entries.put(curTs, entry);
        }

        rw.writeLock().unlock();
    }


    //maintain a version list for each record
    private class VersionEntrySet extends TreeMap<Long, VersionEntry> {
        LogicRid ptr;

        VersionEntrySet(LogicRid ptr) {
//            super(Comparator.comparingLong(VersionEntry::getEndTs));
            this.ptr = ptr;
        }

        public synchronized boolean tryPush(Record record) {
            long xid = TransactionContext.getTransaction().getXid();
            VersionEntry entry = new VersionEntry(xid, record);

            if (this.isEmpty()) {
                this.put(entry.getEndTs(), entry);
                return true;
            }

            var lastentry = this.lastEntry().getValue();
            //存在未提交且不属于当前事务的记录,发生冲突
            if (lastentry.getEndTs() == Long.MAX_VALUE && lastentry.getStartTs() != xid) {
                return false;
            }

            this.put(entry.getEndTs(), entry);
            return true;
        }

        public VersionEntry floor(long endTs) {
            if(floorEntry(endTs)!=null){
                return floorEntry(endTs).getValue();
            }
            return null;
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


