package com.idme.table;

import java.nio.ByteBuffer;

import static com.idme.common.constants.PAGE_SIZE;
import static com.idme.common.constants.SLOT_SIZE;

public class Page {

    byte[] data;
    private boolean isDirty;
    public int pageId;
    private int nextPageId;
    private int freeSpace;
    private final int HEADER_SIZE = Integer.BYTES * 5;
    private int lower;
    private int upper;
//    private int recordCount;
    private ByteBuffer buffer;

    public Page(int id) {
        data = new byte[PAGE_SIZE];
        isDirty = false;
        buffer = ByteBuffer.wrap(data);

        pageId = id;
        nextPageId = Integer.MAX_VALUE;
        lower = HEADER_SIZE;
        upper = PAGE_SIZE;
//        recordCount = 0;
//        head = new Slot(-1,-1);
//        tail = head;
    }

    class Slot {
        //两个int，占用
        int offset;

        int size;
        Slot next;

        public Slot(int offset, int size) {
            this.offset = offset;
            this.size = size;
//          this.next = next;
        }

        public Slot(){}

        public int deserialize(int slotId) {
//            buffer.position(lower-Integer.BYTES*2);
            int offset = HEADER_SIZE+slotId*SLOT_SIZE;

            this.offset = buffer.getInt(offset);
            offset+= Integer.BYTES;
            this.size = buffer.getInt(offset);
            return offset;
        }

        public int serialize(int offset) {
            buffer.putInt(offset, this.offset);
            offset += Integer.BYTES;
            buffer.putInt(offset, this.size);
            offset += Integer.BYTES;
            return offset;
        }
    }

    public int getRecordCount() {
        return (lower - HEADER_SIZE) / SLOT_SIZE;
    }

    Slot[] slots;

    public void setDirty(boolean dirty) {
        isDirty = dirty;
    }

    public boolean isDirty() {
        return isDirty;
    }

    public byte[] getData() {
        return data;
    }

    public int getNextPageId() {
        return nextPageId;
    }

    public void setNextPageId(int nextPageId) {
        this.nextPageId = nextPageId;

    }

    public int getFreeSpace() {
        return upper - lower;
    }

    public void insertRecord(Record record) {
        if (upper - lower < record.size + Integer.SIZE * 2) {
            return;
        }

        //移动upper指针
        upper -= record.getSize();

        //写入slot
        Slot slot = new Slot(upper, record.getSize());
        lower = slot.serialize(lower);

        //写入record
        record.serializeTo(buffer, upper);

        this.setDirty(true);
    }

    public void deleteRecord(int slotId) {
        Slot slot = slots[slotId];
        Record record = new Record();
        record.deserializeFrom(buffer, slot.offset);
        record.setDeleted(true);
        record.serializeTo(buffer, slot.offset);
    }

    public Record getRecord(int slotId) {
        Record record = new Record();
        Slot slot = new Slot();
        slot.deserialize(slotId);
        record.size = slot.size;
        record.deserializeFrom(buffer, slot.offset);
        return record;
    }

    //读取页头信息
    public int serialize() {
        int offset = 0;
        buffer.putInt(offset, pageId);
        offset += Integer.BYTES;
        buffer.putInt(offset, nextPageId);
        offset += Integer.BYTES;
        buffer.putInt(offset, lower);
        offset += Integer.BYTES;
        buffer.putInt(offset, upper);
        offset += Integer.BYTES;
        buffer.putInt(offset, getRecordCount());
        offset += Integer.BYTES;

//        for (int i = 0; i < getRecordCount(); i++) {
//            buffer.putInt(offset, slots[i].offset);
//            offset += Integer.BYTES;
//            buffer.putInt(offset, slots[i].size);
//            offset += Integer.BYTES;
//        }
        return offset;
    }

    //写入页头信息
    public int deserialize() {
        int offset = 0;
        pageId = buffer.getInt(offset);
        offset += Integer.BYTES;
        nextPageId = buffer.getInt(offset);
        offset += Integer.BYTES;
        lower = buffer.getInt(offset);
        offset += Integer.BYTES;
        upper = buffer.getInt(offset);
        offset += Integer.BYTES;
//        recordCount = buffer.getInt(offset);
//        offset += Integer.BYTES;

//        slots = new Slot[recordCount];
//        for (int i = 0; i < recordCount; i++) {
//            slots[i] = new Slot(0, 0);
//            slots[i].offset = buffer.getInt(offset);
//            offset += Integer.BYTES;
//            slots[i].size = buffer.getInt(offset);
//            offset += Integer.BYTES;
//        }
        return offset;
    }

}
