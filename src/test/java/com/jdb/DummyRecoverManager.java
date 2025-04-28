package com.jdb;

import com.jdb.recovery.LogManager;
import com.jdb.recovery.RecoveryManager;
import com.jdb.recovery.logs.LogRecord;
import com.jdb.table.PagePointer;

public class DummyRecoverManager extends RecoveryManager {
    public DummyRecoverManager() {
//        super(null);
        super(new DummyBufferPool());
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
    public long logTrxBegin(long xid) {

        return xid;
    }

    @Override
    public long logUpdate(long xid, int pid, short offset, byte[] oldData, byte[] newData) {

        return xid;
    }

    @Override
    public long logUndoCLR(LogRecord origin) {

        return 0;
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
    public long logCommit(long xid) {

        return 0L;
    }

    @Override
    public long logAbort(long xid) {

        return 0L;
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
    public long logPageAlloc(long xid, long pid) {
        return 0L;
    }

    @Override
    public long logDataPageInit(long xid, long pid) {
        return 0L;
    }

    @Override
    public long logIndexPageInit() {
        return 0L;
    }

    @Override
    public long logPageLink(long xid, long pid, long beforeNextPid, long afterNextPid) {
        return 0L;
    }

    @Override
    public long logCreateFile(long xid, int fid, String fileName) {
        return 0L;
    }

    @Override
    public void restart() {

    }

    @Override
    public int getNextLsn() {
        return 0;
    }

    @Override
    public void init() {

    }

    @Override
    public synchronized void checkpoint() {

    }

}
