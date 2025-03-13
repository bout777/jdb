package com.idme.table;

import java.nio.ByteBuffer;

public class Record {
    public int recordId;
    public int size;
    //暂时只能存int类型
    public int[] value;

    public int serializeTo(ByteBuffer buffer, int offset) {
        buffer.position(offset);
        for (int v : value) {
            buffer.putInt(offset, v);
            offset += Integer.BYTES;
        }
        return offset;
    }

    public int deserializeFrom(ByteBuffer buffer, int offset) {
        buffer.position(offset);
        for (int i = 0; i < value.length; i++) {
            value[i] = buffer.getInt(offset);
            offset += Integer.BYTES;
        }
        return offset;
    }


}
