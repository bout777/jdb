package com.idme.table;

import com.idme.catalog.ColumnDef;
import com.idme.catalog.ColumnList;
import com.idme.common.Value;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Record {
    public int primaryKey;
    public Byte isDeleted;
    public int size;
    //暂时只能存int类型
    public List<Value> values;

    public Record() {
        values = new ArrayList<>();
//        this.size = Byte.BYTES + Integer.BYTES + value.length * Integer.BYTES;
    }

    public int getPrimaryKey() {
        return primaryKey;
    }

    public int serializeHeader(ByteBuffer buffer, int offset) {
        buffer.put(offset, isDeleted);
        offset += Byte.BYTES;

        buffer.putInt(offset, primaryKey);
        offset += Integer.BYTES;

        buffer.putInt(offset, size);
        offset += Integer.BYTES;

        return offset;
    }

    public int deserializeHeader(ByteBuffer buffer, int offset) {
        isDeleted = buffer.get(offset);
        offset += Byte.BYTES;

        primaryKey = buffer.getInt(offset);
        offset += Integer.BYTES;

        size = buffer.getInt(offset);
        offset += Integer.BYTES;

        return offset;
    }

    public int serializeTo(ByteBuffer buffer, int offset) {
        offset = serializeHeader(buffer, offset);

        for (Value v : values) {
            offset = v.serialize(buffer, offset);
        }

        return offset;
    }

    public int deserializeFrom(ByteBuffer buffer, int offset, ColumnList columnList) {
        offset = deserializeHeader(buffer, offset);
        //TODO 这里暂时写死，后期要改
//        value = new int[10];

//        for (int i = 0; i < value.length; i++) {
//            value[i] = buffer.getInt(offset);
//            offset += Integer.BYTES;
//        }
        for (ColumnDef def : columnList.columns()) {
            Value value = Value.deserialize(buffer, offset, def.getType());
            values.add(value);
            offset += value.getBytes();
        }

        return offset;
    }

    public int getSize() {
        return size;
    }

    public boolean isDeleted() {
        return isDeleted == (byte) 1;
    }

    public void setDeleted(boolean flag) {
        isDeleted = flag ? (byte) 1 : (byte) 0;
    }

    @Override
    public String toString() {
        return String.format("Record{primaryKey=%d, isDeleted=%d, size=%d, values=%s}", primaryKey, isDeleted, size, values);
    }


}
