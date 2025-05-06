package com.jdb.index;

import com.jdb.common.value.Value;

import java.util.List;

public interface Index {
    IndexEntry searchEqual(Value<?> key);

    List<IndexEntry> searchRange(Value<?> low, Value<?> high);

    void insert(IndexEntry entry, boolean shouldLog);

    void delete(Value<?> key, boolean shouldLog);

}

