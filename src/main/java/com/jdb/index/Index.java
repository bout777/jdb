package com.jdb.index;

import com.jdb.common.Value;

public interface Index {
    IndexEntry searchEqual(Value<?> key);

    IndexEntry searchRange(Value<?> low, Value<?> high);

    void insert(IndexEntry entry);

    void delete(IndexEntry entry);

}

