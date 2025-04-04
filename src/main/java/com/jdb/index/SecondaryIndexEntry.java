package com.jdb.index;

import com.jdb.common.Value;
import com.jdb.table.RecordID;

public class SecondaryIndexEntry extends IndexEntry {
    Value<?> key;
    RecordID pointer;

    public SecondaryIndexEntry(Value<?> key, RecordID pointer) {
        this.key = key;
        this.pointer = pointer;
    }

    @Override
    public Value<?> getKey() {
        return key;
    }

    @Override
    public Object getValue() {
        return pointer;
    }

    @Override
    public int getBytes() {
        return key.getBytes() + RecordID.SIZE;
    }
}
