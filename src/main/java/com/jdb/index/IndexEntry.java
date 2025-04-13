package com.jdb.index;

import com.jdb.common.Value;
import com.jdb.table.PagePointer;

/**
 * The type Index entry.
 */
public abstract class IndexEntry implements Comparable<IndexEntry> {

    /**
     * Gets key.
     *
     * @return the key
     */
    public abstract Value<?> getKey();

    /**
     * Gets value.
     *
     * @return the value
     */
    public abstract Object getValue();

    @Override
    public int compareTo(IndexEntry o) {
        return getKey().compareTo(o.getKey());
    }

    public PagePointer getPointer() {
        return null;
    }

    /**
     * Gets bytes.
     *
     * @return the bytes
     */
    public abstract int getBytes();
}
