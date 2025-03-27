package com.jdb.index;

import com.jdb.common.Value;

public abstract class IndexEntry implements Comparable<IndexEntry> {

    public abstract Value<?> getKey();

    public abstract Object getValue();

    @Override
    public int compareTo(IndexEntry o) {
        return getKey().compareTo(o.getKey());
    }

    public abstract int getBytes();
}
