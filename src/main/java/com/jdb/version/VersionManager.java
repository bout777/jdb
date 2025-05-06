package com.jdb.version;

import com.jdb.Engine;
import com.jdb.common.value.Value;
import com.jdb.exception.WriteConflictException;
import com.jdb.table.RowData;
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

//    private static VersionManager instance = new VersionManager();
//
//    public synchronized static  VersionManager getInstance() {
//        if (instance == null)
//            instance = new VersionManager();
//        return instance;
//    }

    private  Engine engine;
    private final ReadWriteLock rw = new ReentrantReadWriteLock();
    private Map<LogicRid, VersionEntrySet> versionMap = new HashMap<>();

    public VersionManager(Engine engine) {
        this.engine = engine;
    }

    public void injectDependency() {

    }


    //
    public void pushUpdate(String tableName, RowData rowData) {
        var rid = new LogicRid(tableName, rowData.getPrimaryKey());

        var deque = versionMap.get(rid);
        /*
        todo 如果vmMap没有该记录的版本链，先把原版本加一份到这个版本链里，
            否则会出现一个事务更新了但还没提交，其他事务都查不到原来的版本
         */
        if (deque == null) {
            deque = new VersionEntrySet(rid);
            versionMap.put(rid, deque);
        }
        if (!deque.tryPush(rowData)) {
            //todo 冲突回滚
            throw new WriteConflictException("write conflict,abort needed");
        }

        var curTrx = TransactionContext.getTransaction();
        curTrx.getWriteSet().add(rid);
    }

    public ReadResult read(String tableName, Value<?> key) {
        int primaryKey = key.getValue(Integer.class);
        rw.readLock().lock();
        var rid = new LogicRid(tableName, key);
        var entrySet = versionMap.get(rid);
        if (entrySet == null)
            return ReadResult.notPresent();

        var isolevel = TransactionContext.getTransaction().getIsolationLevel();
        RowData rowData = null;
        long xid = TransactionContext.getTransaction().getXid();
        switch (isolevel) {
            case READ_COMMITTED -> {
                long curTs = TransactionManager.getCurrentTrxStamp();
                var entry = entrySet.getVisibleVersion(curTs);
                if(entry!=null) {
                    rowData = entry.getRecord();
                }
            }
            case SNAPSHOT_ISOLATION -> {
                var entry = entrySet.getVisibleVersion(xid);
                if(entry!=null)
                    rowData = entry.getRecord();
            }
        }
        rw.readLock().unlock();
        if(rowData ==null)
            return ReadResult.invisible();
        else
            return ReadResult.visible(rowData);
    }


    public void commit(Set<LogicRid> writeSet) {
        long curTs = TransactionManager.getCurrentTrxStamp();
        //事务提交期间，阻塞读线程
        rw.writeLock().lock();

        for (LogicRid ptr : writeSet) {
            var entries = versionMap.get(ptr);
            var entry = entries.pollLastEntry().getValue();
            entry.setEndTs(curTs);
            entries.put(curTs, entry);
        }

        rw.writeLock().unlock();
    }

    public void cleanup(Set<LogicRid> writeSet) {
        for (LogicRid ptr : writeSet){
            var entries = versionMap.get(ptr);
            entries.pollLastEntry();
        }
    }


    //maintain a com.jdb.version list for each record
    private class VersionEntrySet extends TreeMap<Long, VersionEntry> {
        LogicRid ptr;

        VersionEntrySet(LogicRid ptr) {
//            super(Comparator.comparingLong(VersionEntry::getEndTs));
            this.ptr = ptr;
        }

        public synchronized boolean tryPush(RowData rowData) {
            long xid = TransactionContext.getTransaction().getXid();
            VersionEntry entry = new VersionEntry(xid, rowData);

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

        public VersionEntry getVisibleVersion(long ts) {
            var latestEntry = lastEntry().getValue();
            long xid = TransactionContext.getTransaction().getXid();
            if(latestEntry.getStartTs()==xid)
                return latestEntry;

            if(floorEntry(ts)!=null){
                return floorEntry(ts).getValue();
            }
            return null;
        }
    }
}


