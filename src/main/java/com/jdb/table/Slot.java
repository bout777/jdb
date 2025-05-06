package com.jdb.table;

import java.nio.ByteBuffer;

public class Slot {
    //两个int，占用
    int offset;

    int size;
//    int primaryKey;

    public Slot(int offset, int size) {
        this.offset = offset;
        this.size = size;
//          this.next = next;
//        this.primaryKey = primaryKey;
    }

    public Slot() {
    }

    public static Slot deserialize(int offset, ByteBuffer buffer) {
        Slot slot = new Slot();
        slot.offset = buffer.getInt(offset);
        offset += Integer.BYTES;
        slot.size = buffer.getInt(offset);
//        offset += Integer.BYTES;
//        slot.primaryKey = buffer.getInt(offset);
        return slot;
    }

//    public Value getPrimaryKey() {
//        return primaryKey;
//    }

    public void serialize(int offset, ByteBuffer buffer) {
        buffer.putInt(offset, this.offset);
        offset += Integer.BYTES;
        buffer.putInt(offset, this.size);
//        offset += Integer.BYTES;
//        buffer.putInt(offset, this.primaryKey);
    }
}
