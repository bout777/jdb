package com.jdb.index;

import com.jdb.common.Value;

import java.util.List;

public interface Index {
    IndexEntry searchEqual(Value<?> key);

    List<IndexEntry> searchRange(Value<?> low, Value<?> high);

    void insert(IndexEntry entry);

    void delete(IndexEntry entry);

}

