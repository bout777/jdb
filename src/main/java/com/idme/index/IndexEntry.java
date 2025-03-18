package com.idme.index;

import com.idme.common.Value;
import com.idme.table.PagePointer;

public class IndexEntry implements Comparable<IndexEntry> {
    Value<?> key;
    PagePointer pointer;

    public IndexEntry(Value<?> key, PagePointer pointer) {
        this.key = key;
        this.pointer = pointer;
    }

    public Value<?> getKey() {
        return key;
    }

    @Override
    public int compareTo(IndexEntry o) {
        return this.key.compareTo(o.key);
    }
}
