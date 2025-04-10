package com.jdb.version;

import com.jdb.table.Record;
/*
* 快照隔离级别
* 每次修改会创建一个entry，
* startTs设置为事务的xid，endTs设置为INF(无穷)
* 当事务想读取某行数据，会从版本链的头部开始扫描
* 读取endTs<=xid中具有最大endTs的记录,
* 对于写-写冲突，采用先更新者获胜策略
* 当事务想要更新某条记录时，先获取这个记录的锁，
* 检查队列头的endTs是否小于xid
* 如果是，则追加entry，反之更新失败，回滚。
* 释放锁
*
*
* 读已提交级别
* 在读取的时候，从版本链头部开始扫描
* 读取endTs<=curTs(当前预分配的事务id)的记录
* 其他操作和快照隔离相同
*
* Q：为什么需要在写入时加锁？
* A：为了保证 读取队头-写入 这个操作的原子性
*
* Q：会产生死锁吗？
* A：不会，因为这个锁不同于2PL里面的锁，修改完成后就马上释放。
* */
public class VersionEntry {
    //the trx that insert or update on this record
    public long startTs;

    public long endTs = Long.MAX_VALUE;
    //content
    public Record record;

    public VersionEntry(long startTs, Record record) {
        this.startTs= startTs;
        this.record = record;
    }

    public Record getRecord() {
        return record;
    }

    public long getStartTs() {
        return startTs;
    }

    public long getEndTs() {
        return endTs;
    }

    public void setEndTs(long endTs) {
        this.endTs = endTs;
    }

    @Override
    public String toString() {
        return "VersionEntry{" +
                "startTs=" + startTs +
                ", endTs=" + endTs +
                ", record=" + record +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VersionEntry that = (VersionEntry)o;
        return startTs == that.startTs &&
                endTs == that.endTs &&
                record.equals(that.record);
    }


}
