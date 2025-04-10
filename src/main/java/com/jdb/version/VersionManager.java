package com.jdb.version;

import com.jdb.table.Record;
import com.jdb.table.RecordID;
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
    private Map<RecordID, VersionEntrySet> versionMap = new HashMap<>();

    public RecordID pushUpdate(RecordID rid, Record record) {
        var deque = versionMap.get(rid);
        if (deque == null) {
            deque = new VersionEntrySet(rid);
            versionMap.put(rid, deque);
        }
        if (!deque.tryPush(record)) {
            //todo 冲突回滚
            throw new RuntimeException("测试代码不该跑到这,tell me why?");
        }

        return rid;
    }

    public Record read(RecordID rid) {
        rw.readLock().lock();

        var entrySet = versionMap.get(rid);
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

    public void commit(Set<RecordID> writeSet) {
        long curTs = TransactionManager.getCurrentTrxStamp();
        //事务提交期间，阻塞读线程
        rw.writeLock().lock();

        //这里用了O(klogn),后续可以优化成O(k)？
        for (RecordID rid : writeSet) {
            var deque = versionMap.get(rid);
            for (VersionEntry entry : deque) {
                if (entry.getStartTs() == curTs) {
                    entry.setEndTs(TransactionManager.getCurrentTrxStamp());
                }
            }
        }

        rw.writeLock().unlock();
    }


    //都用上java了，不要省内存了，多快好省，干就完了。

    //maintain a version list for each record
    private class VersionEntrySet extends TreeSet<VersionEntry> {
        RecordID rid;
        VersionEntrySet(RecordID rid) {
            super(Comparator.comparingLong(VersionEntry::getEndTs));
            this.rid = rid;
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
