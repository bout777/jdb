package com.jdb.transaction;

public class Transaction {
    private long xid;
    private long lsn;
    private Status status = Status.RUNNING;

    public long getTransactionId() {
        return xid;
    }

    public Status getStatus() {
        return status;
    }

    /**
     * 设置事务状态
     * <p>
     * 不应该被事务的使用者调用，事务的状态应该由recover模块管理
     *
     * @param status
     */
    public void setStatus(Status status) {
        this.status = status;
    }

    public void commit() {
        if (this.status != Status.RUNNING) {
            throw new IllegalStateException("Transaction is not in running state, cannot commit");
        }
        //TODO
    }

    public void abort() {
        if (this.status != Status.RUNNING) {
            throw new IllegalStateException("Transaction is not in running state, cannot abort");
        }
        //TODO
    }

    /**
     * 当事务结束，清理相关资源
     */
    public void cleanup() {
        //TODO
    }

    public enum Status {
        RUNNING,
        COMMITTING,
        ABORTING,
        COMPLETE,
        /**
         * 用于标记恢复过程中需要回滚的事务
         * 数据库发生崩溃时(eg. power off)
         * 当时未提交的事务在重启后都需要在恢复过程中回滚
         */
        RECOVERY_ABORTING;

    }
}
