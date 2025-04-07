package com.jdb.transaction;

public class TransactionContext {
    private static ThreadLocal<TransactionContext> threadTrx = new ThreadLocal<>();

    private long xid;
    public static TransactionContext getTransaction() {
        return threadTrx.get();
    }

    public TransactionContext(long xid) {
        this.xid = xid;
    }

    public static void setTransactionContext(TransactionContext transactionContext) {
        threadTrx.set(transactionContext);
    }

    public static void unsetTransaction() {
        threadTrx.remove();
    }

    public long getXid() {
        return xid;
    }


}
