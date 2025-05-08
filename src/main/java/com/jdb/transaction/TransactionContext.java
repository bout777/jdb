package com.jdb.transaction;

import com.jdb.exception.DatabaseException;
import com.jdb.version.LogicRid;

import java.util.HashSet;
import java.util.Set;

public class TransactionContext {
    private static final ThreadLocal<TransactionContext> threadTrx = new ThreadLocal<>();
    private final long xid;
    private final Set<LogicRid> writeSet = new HashSet<>();
    private IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;


    public TransactionContext(long xid) {
        this.xid = xid;
    }
    public TransactionContext(long xid, IsolationLevel isolationLevel) {
        this.xid = xid;
        this.isolationLevel = isolationLevel;
    }

    public static TransactionContext getTransaction() {
        return threadTrx.get();
    }

    public static void setTransactionContext(TransactionContext transactionContext) {
        if (threadTrx.get() != null) {
            throw new DatabaseException("当前线程上正在运行事务：" + threadTrx.get());
        }
        threadTrx.set(transactionContext);
    }

    public static void unsetTransaction() {
        threadTrx.remove();
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
