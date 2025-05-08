package com.jdb.index;

import com.jdb.common.value.Value;
import com.jdb.table.PagePointer;
import com.jdb.table.RowData;

public class ClusterIndexEntry extends IndexEntry {
    Value<?> key;
    RowData rowData;
    PagePointer ptr;

    public ClusterIndexEntry(Value<?> key, RowData rowData) {
        this.key = key;
        this.rowData = rowData;
    }

    public ClusterIndexEntry(Value<?> key, RowData rowData, PagePointer ptr) {
        this.key = key;
        this.rowData = rowData;
        this.ptr = ptr;
    }

    public RowData getRecord() {
        return rowData;
    }

    public void setRecord(RowData rowData) {
        this.rowData = rowData;
    }

    public void setPagePointer(PagePointer ptr) {
        this.ptr = ptr;
    }

    @Override
    public PagePointer getPointer() {
        return ptr;
    }

    @Override
    public Value<?> getKey() {
        return key;
    }

    @Override
    public Object getValue() {
        return rowData;
    }

    @Override
    public int getBytes() {
        return key.getBytes() + rowData.size();
    }

    @Override
    public String toString() {
        return "ClusterIndexEntry{" +
                "key=" + key +
                ", record=" + rowData +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterIndexEntry that = (ClusterIndexEntry) o;
        return key.equals(that.key) &&
                rowData.equals(that.rowData);
    }
}
