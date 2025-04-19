package com.jdb;

import com.jdb.recovery.LogManager;
import com.jdb.recovery.RecoveryManager;
import com.jdb.recovery.logs.LogRecord;
import com.jdb.storage.BufferPool;
import com.jdb.table.PagePointer;

public class DummyRecoverManager extends RecoveryManager {
    public DummyRecoverManager() {
        super(null);
    }

    @Override
    public void setLogManager(LogManager logManager) {

    }

    @Override
    public LogManager getLogManager() {
        return null;
    }

    @Override
    public void setEngine(Engine engine) {

    }

    @Override
    public synchronized void registerTransaction(long xid) {

    }

    @Override
    public void logTrxBegin(long xid) {

    }

    @Override
    public void logUpdate(long xid, int pid, short offset, byte[] oldData, byte[] newData) {

    }

    @Override
    public void logUndoCLR(LogRecord origin) {

    }

    @Override
    public synchronized long logInsert(long xid, PagePointer ptr, byte[] image) {
        return -1L;
    }

    @Override
    public long logDelete(long xid, PagePointer ptr, byte[] image) {
        return -1L;
    }

    @Override
    public void logCommit(long xid) {

    }

    @Override
    public void logAbort(long xid) {

    }

    @Override
    public void rollback(long xid) {

    }

    @Override
    public void flush2lsn(long lsn) {

    }

    @Override
    public void dirtyPage(long pid, long lsn) {

    }

    @Override
    public synchronized void checkpoint() {
        super.checkpoint();
    }

}
