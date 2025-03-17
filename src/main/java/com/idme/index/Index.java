package com.idme.index;

import com.idme.common.Value;

public interface Index {
    public IndexEntry searchEqual(Value<?> key);
    public IndexEntry searchRange(Value<?> low, Value<?> high);
    public void insert(IndexEntry entry);
    public void delete(IndexEntry entry);

}

