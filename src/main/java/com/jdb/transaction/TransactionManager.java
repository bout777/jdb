package com.jdb.transaction;

import com.jdb.Engine;
import com.jdb.recovery.RecoveryManager;
import com.jdb.version.VersionManager;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionManager {
    //预分配的xid，递增
    private static final AtomicLong trxStartStamp = new AtomicLong(1);
    //懒加载方法，测试用
//    private static TransactionManager instance ;
//    public synchronized static TransactionManager getInstance() {
//        if(instance == null)
//            instance = new TransactionManager(RecoveryManager.getInstance());
//        return instance;
//    }
    private Engine engine;
    private RecoveryManager recoveryManager;
    private VersionManager versionManager;
    private Map<Integer, Transaction> activeTransactions;
    //xid->writeSet
//    private Map<Long, Set<LogicRid>> writeSetMap;
    //返回xid

    public TransactionManager(RecoveryManager rm, VersionManager vm) {
        recoveryManager = rm;
        versionManager = vm;
    }

    public TransactionManager(Engine engine) {
        this.engine = engine;
    }

    public static long getCurrentTrxStamp() {
        return trxStartStamp.get();
    }

    public void injectDependency() {
        recoveryManager = engine.getRecoveryManager();
        versionManager = engine.getVersionManager();
    }

    public long begin() {
        long xid = trxStartStamp.getAndIncrement();
        TransactionContext.setTransactionContext(new TransactionContext(xid));
        recoveryManager.registerTransaction(xid);
        return xid;
    }

    public void commit() {
        var writeSet = TransactionContext.getTransaction().getWriteSet();
        if (writeSet != null) {
            versionManager.commit(writeSet);
        }

        // 调用recover写日志
        recoveryManager.logCommit(TransactionContext.getTransaction().getXid());

        //事后清理
        TransactionContext.unsetTransaction();
    }

    /**
     * 逻辑故障下的事务回滚
     * 从事务信息中取出最新的已经写入的lsn
     * 从该lsn往前读取log
     * 如果这个log属于当前事务，执行undo
     * 追加redo-only日志
     * 当读取到该事务的begin日志时停止回滚
     *
     *
     */
    public void abort() {
        long xid = TransactionContext.getTransaction().getXid();
        recoveryManager.rollback(xid);

        var writeSet = TransactionContext.getTransaction().getWriteSet();
        if (writeSet != null) {
            versionManager.cleanup(writeSet);
        }

        TransactionContext.unsetTransaction();
    }
}

