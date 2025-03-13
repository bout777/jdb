package com.idme.table;

import java.nio.ByteBuffer;

import static com.idme.common.constants.*;

public class Page {

    byte[] data;
    private boolean isDirty;
    private int freeSpace;
    private int lower;
    private int upper;
    private ByteBuffer buffer;
    public Page(){
        data = new byte[PAGE_SIZE];
        isDirty = false;
        buffer = ByteBuffer.wrap(data);
        upper = PAGE_SIZE;
        head = new Slot(-1,-1);
        tail = head;
    }
    class Slot{
        //两个int，占用
        int offset;

        int size;
        Slot next;
        public Slot(int offset, int size) {
            this.offset = offset;
            this.size = size;
//          this.next = next;
        }
        public void deserializeFrom() {
//            buffer.position(lower-Integer.BYTES*2);
            offset = buffer.getInt();
            buffer.position(lower-Integer.BYTES);
            size = buffer.getInt();
        }
    }
    Slot head;
    Slot tail;
    public void setDirty(boolean dirty) {
        isDirty = dirty;
    }
    public boolean isDirty() {
        return isDirty;
    }
    public byte[] getData() {
        return data;
    }
    public void insertRecord(Record record){
        if(upper-lower<record.size+Integer.SIZE*2){
            return;
        }
        //创建slot
        lower += SLOT_SIZE;
        Slot slot = new Slot(lower,record.size);
        //添加到链表
        tail.next = slot;
        tail = slot;
        //写入slot

        //移动upper指针
        upper -= record.size;
        //写入record
        record.serializeTo(buffer,upper);
        this.setDirty(true);
    }
    public void deleteRecord(Record record){

    }
    public void serialize(){}

    public void deserialize(){}

}
