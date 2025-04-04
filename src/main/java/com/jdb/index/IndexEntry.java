package com.jdb.index;

import com.jdb.common.Value;

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

    /**
     * Gets bytes.
     *
     * @return the bytes
     */
    public abstract int getBytes();
}
