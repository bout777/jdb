package com.jdb.transaction;

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



    private long xid;
    private IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;

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

    @Override
    public String toString() {
        return "TransactionContext{" +
                "xid=" + xid +
                ", isolationLevel=" + isolationLevel +
                '}';
    }


}
