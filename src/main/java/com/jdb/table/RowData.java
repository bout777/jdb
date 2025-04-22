package com.jdb.table;

import com.jdb.catalog.Schema;
import com.jdb.common.Value;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RowData {
    public int size;
    public List<Value> values;

    public RowData(Value ...values) {
        this(Arrays.asList(values));
    }

    public RowData(List<Value> values) {
        this.values = values;
        int size = 0;
        for (var value : values) {
            size += value.getBytes();
        }
        this.size = size;
    }

    private RowData(List<Value> values, int size) {
        this.values = values;
        this.size = size;
    }

    public Value<?> getPrimaryKey() {
        return values.get(0);
    }

    public int serialize(ByteBuffer buffer, int offset) {
        for (var value : values) {
            offset = value.serialize(buffer, offset);
        }
        return offset;
    }

    public static RowData deserialize(ByteBuffer buffer, int offset, Schema schema) {
        List<Value> values = new ArrayList<>();
        int size = offset;
        for (var column : schema.columns()) {
            var value = Value.deserialize(buffer, offset, column.getType());
            values.add(value);
            offset += value.getBytes();
        }
        size = offset - size;
        return new RowData(values, size);
    }

    public int size() {
        return size;
    }


    @Override
    public String toString() {
        return String.format("Record{primaryKey=%s, size=%d, values=%s}",getPrimaryKey(), size, values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RowData that = (RowData) o;
        return values.equals(that.values) &&
                size() == that.size() &&
                getPrimaryKey().equals(that.getPrimaryKey());

    }


}
