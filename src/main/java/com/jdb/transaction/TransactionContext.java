package com.jdb.transaction;

import com.jdb.version.LogicRid;

import java.util.HashSet;
import java.util.Set;

public class TransactionContext {
    private static ThreadLocal<TransactionContext> threadTrx = new ThreadLocal<>();

    public static TransactionContext getTransaction() {
        return threadTrx.get();
    }
    public static void setTransactionContext(TransactionContext transactionContext) {
        if(threadTrx.get()!=null){
            throw new RuntimeException("当前线程上正在运行事务："+threadTrx.get());
        }
        threadTrx.set(transactionContext);
    }

    public static void unsetTransaction() {
        threadTrx.remove();
    }



    private final long xid;
    private IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;
    private final Set<LogicRid> writeSet = new HashSet<>();

    public TransactionContext(long xid) {
        this.xid = xid;
    }

    public TransactionContext(long xid, IsolationLevel isolationLevel) {
        this.xid = xid;
        this.isolationLevel = isolationLevel;
    }

    public long getXid() {
        return xid;
    }

    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    public Set<LogicRid> getWriteSet() {
        return writeSet;
    }

    @Override
    public String toString() {
        return "TransactionContext{" +
                "xid=" + xid +
                ", isolationLevel=" + isolationLevel +
                '}';
    }


}
