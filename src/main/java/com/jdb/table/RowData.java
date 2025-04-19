package com.jdb.table;

import com.jdb.catalog.Column;
import com.jdb.catalog.Schema;
import com.jdb.common.Value;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RowData {
    //    public int primaryKey;
//    public Byte isDeleted;
    public int size;
    //暂时只能存int类型
    public List<Value> values;

    public RowData(List<Value> values, int size) {
        this.values = values;
        this.size = size;
    }

    public Value<?> getPrimaryKey() {
        return values.get(0);
    }

    public int serializeHeader(ByteBuffer buffer, int offset) {
//        buffer.put(offset, isDeleted);
//        offset += Byte.BYTES;

//        buffer.putInt(offset, primaryKey);
//        offset += Integer.BYTES;

//        buffer.putInt(offset, size);
//        offset += Integer.BYTES;

        return offset;
    }

    public int deserializeHeader(ByteBuffer buffer, int offset) {
//        isDeleted = buffer.get(offset);
//        offset += Byte.BYTES;

//        primaryKey = buffer.getInt(offset);
//        offset += Integer.BYTES;

//        size = buffer.getInt(offset);
//        offset += Integer.BYTES;

        return offset;
    }

    public int serializeTo(ByteBuffer buffer, int offset) {
        for (var value : values) {
            offset = value.serialize(buffer, offset);
        }
        return offset;
    }

    private int deserializeFrom(ByteBuffer buffer, int offset, Schema schema) {
//        offset = deserializeHeader(buffer, offset);
        //TODO 这里暂时写死，后期要改
        for (Column def : schema.columns()) {
            Value value = Value.deserialize(buffer, offset, def.getType());
            values.add(value);
            offset += value.getBytes();
        }

        return offset;
    }

    public static RowData deserialize(ByteBuffer buffer, int offset, Schema schema) {
//        RowData rowData = new RowData();
        List<Value> values = new ArrayList<>();
        int size = offset;
        for (var column : schema.columns()) {
            var value = Value.deserialize(buffer, offset, column.getType());
            values.add(value);
            offset += value.getBytes();
        }
        size = offset - size;
        //        rowData.deserializeFrom(buffer, offset, schema);
        return new RowData(values, size);
    }

    public int getSize() {
        return size;
    }

//    public boolean isDeleted() {
//        return isDeleted == (byte) 1;
//    }
//
//    public void setDeleted(boolean flag) {
//        isDeleted = flag ? (byte) 1 : (byte) 0;
//    }

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
                getSize() == that.getSize() &&
                getPrimaryKey().equals(that.getPrimaryKey());

    }


}
