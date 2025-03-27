package com.jdb.index;

import com.jdb.common.Value;
import com.jdb.table.Record;

public class ClusterIndexEntry extends IndexEntry {
    Value<?> key;
    Record record;
    public ClusterIndexEntry(Value<?> key, Record record) {
        this.key = key;
        this.record = record;
    }

    public Record getRecord() {
        return record;
    }

    public void setRecord(Record record) {
        this.record = record;
    }


    @Override
    public Value<?> getKey() {
        return key;
    }

    @Override
    public Object getValue() {
        return record;
    }

    @Override
    public int getBytes() {
        return key.getBytes() + record.getSize();
    }

    @Override
    public String toString() {
        return "ClusterIndexEntry{" +
                "key=" + key +
                ", record=" + record +
                '}';
    }
}
