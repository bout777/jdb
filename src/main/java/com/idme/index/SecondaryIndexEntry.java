package com.idme.index;

import com.idme.common.Value;
import com.idme.table.PagePointer;

public class SecondaryIndexEntry extends IndexEntry {
    Value<?> key;
    PagePointer pointer;

    public SecondaryIndexEntry(Value<?> key, PagePointer pointer) {
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
        return key.getBytes() + PagePointer.SIZE;
    }
}
