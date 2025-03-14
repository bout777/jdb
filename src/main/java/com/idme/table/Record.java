package com.idme.table;

import java.nio.ByteBuffer;

public class Record {
    public int primaryKey;
    public Byte isDeleted;
    public int size;
    //暂时只能存int类型
    public int[] value;
    public Record() {

//        this.size = Byte.BYTES + Integer.BYTES + value.length * Integer.BYTES;
    }

    public int getPrimaryKey() {
        return primaryKey;
    }

    public int serializeTo(ByteBuffer buffer, int offset) {
        buffer.put(offset,isDeleted);
        offset += Byte.BYTES;
        buffer.putInt(offset,primaryKey);
        offset += Integer.BYTES;
        buffer.putInt(offset, size);
        offset += Integer.BYTES;
        for (int v : value) {
            buffer.putInt(offset, v);
            offset += Integer.BYTES;
        }
        return offset;
    }

    public int deserializeFrom(ByteBuffer buffer, int offset) {
        isDeleted = buffer.get(offset);
        offset += Byte.BYTES;
        primaryKey = buffer.getInt(offset);
        offset += Integer.BYTES;
        size = buffer.getInt(offset);
        offset += Integer.BYTES;
        //TODO 这里暂时写死，后期要改
        value = new int[10];
        for (int i = 0; i < value.length; i++) {
            value[i] = buffer.getInt(offset);
            offset += Integer.BYTES;
        }
        return offset;
    }

    public int getSize() {
        return size;
    }

    public void setDeleted(boolean flag) {
        isDeleted = flag ? (byte) 1 : (byte) 0;
    }

    public boolean isDeleted() {
        return isDeleted == (byte)1;
    }


}
