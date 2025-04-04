package com.jdb.transaction;

public class TransactionContext {
    private ThreadLocal<TransactionContext> threadTrx = new ThreadLocal<>();

    public TransactionContext getTransactionContext() {
        return threadTrx.get();
    }

    public void setTransactionContext(TransactionContext transactionContext) {
        this.threadTrx.set(transactionContext);
    }

    public void unsetTransaction() {
        this.threadTrx.remove();
    }
}
