package com.jdb.transaction;

import com.jdb.recovery.RecoveryManager;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionManager {
    static AtomicInteger xid = new AtomicInteger(1);
    private RecoveryManager recoveryManager;
    private Map<Integer, Transaction> activeTransactions;

    public int getNextXid() {
        return 0;
    }

    //返回xid
    public int begin() {
        return 0;
    }

    public void commit(long xid) {
    }

    /**
     * 逻辑故障下的事务回滚
     * 从事务信息中取出最新的已经写入的lsn
     * 从该lsn往前读取log
     * 如果这个log属于当前事务，执行undo
     * 追加redo-only日志
     * 当读取到该事务的begin日志时停止回滚
     *
     * @param xid
     */
    public void abort(long xid) {

    }
}

