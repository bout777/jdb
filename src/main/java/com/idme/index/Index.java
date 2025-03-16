package com.idme.index;

public interface Index {
    public IndexEntry searchEqual(Key key);
    public IndexEntry searchRange(Key low,Key high);
    public void insert(IndexEntry entry);
    public void delete(IndexEntry entry);

}
