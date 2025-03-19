package com.idme.table;

import com.idme.catalog.ColumnList;
import com.idme.storage.BufferPool;
import com.idme.storage.Page;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.idme.common.Constants.PAGE_SIZE;
import static com.idme.common.Constants.SLOT_SIZE;
/*
*
* TODO: 添加空闲槽位列表，继续优化插入逻辑
* 
*
*
*
*
*
*
* */
public class DataPage {

    private final int HEADER_SIZE = Integer.BYTES * 5;
    public int pageId;
    byte[] data;
    List<Slot> slots = new ArrayList<>();
    private boolean isDirty;
    private int nextPageId;
    private int lower;
    private int upper;
    private int recordCount;
    private final ByteBuffer buffer;
    private Page page;

    public DataPage(int id, Page page) {
        data = page.getData();
        isDirty = false;
        buffer = ByteBuffer.wrap(data);
        pageId = id;
        this.page = page;
        //从page中读取页头信息，槽位数组
        deserialize();
    }

    public void init(){
        nextPageId = Integer.MAX_VALUE;
        lower = HEADER_SIZE;
        upper = PAGE_SIZE;
        serializeHeader();
    }

    public int getRecordCount() {
        return (lower - HEADER_SIZE) / SLOT_SIZE;
    }

    public boolean isDirty() {
        return page.isDirty();
    }

    public void setDirty(boolean dirty) {
        page.setDirty(dirty);
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

    public List<PagePointer> getPointers() {
        List<PagePointer> pointers = new ArrayList<>();

        for (int i = 0; i < slots.size(); i++) {
            PagePointer pointer = new PagePointer(pageId, i);
            pointers.add(pointer);
        }

        return pointers;
    }

    public List<Slot> getSlots() {
        return slots;
    }

    public int getFreeSpace() {
        return upper - lower;
    }

    public void insertRecord(Record record) {

        //移动upper指针
        upper -= record.getSize();

        //写入slot
        Slot slot = new Slot(upper, record.getSize(), record.getPrimaryKey());
        insertSlot(slot);
        lower += SLOT_SIZE;

        //写入record
        record.serializeTo(buffer, upper);
        serializeHeader();
        this.setDirty(true);
    }

    public void deleteRecord(int slotId) {
        Slot slot = slots.get(slotId);
        Record record = new Record();
        record.deserializeHeader(buffer, slot.offset);
        record.setDeleted(true);
        record.serializeHeader(buffer, slot.offset);
    }

    public Record getRecord(int slotId, ColumnList columnList) {
        Record record = new Record();
        Slot slot = new Slot();
        slot.deserialize(slotId);
        record.size = slot.size;
        record.deserializeFrom(buffer, slot.offset, columnList);
        return record;
    }

    public void serialize() {
        serializeHeader();
        serializeSlots();
    }

    private void serializeHeader() {
        int offset = 0;
        buffer.putInt(offset, pageId);

        offset += Integer.BYTES;
        buffer.putInt(offset, nextPageId);

        offset += Integer.BYTES;
        buffer.putInt(offset, lower);

        offset += Integer.BYTES;
        buffer.putInt(offset, upper);

    }

    private void deserializeHeader() {
        int offset = 0;
//        pageId = buffer.getInt(offset);
        offset += Integer.BYTES;

        nextPageId = buffer.getInt(offset);
        offset += Integer.BYTES;

        lower = buffer.getInt(offset);
        offset += Integer.BYTES;

        upper = buffer.getInt(offset);
        offset += Integer.BYTES;
    }

    private void serializeSlots() {
        int n = getRecordCount();
        for (int i = 0; i < n; i++) {
            slots.get(i).serialize(i);
        }
    }

    private void deserializeSlots() {
        if (!slots.isEmpty())
            throw new RuntimeException("slots is not empty");

        int n = getRecordCount();
        for (int i = 0; i < n; i++) {
            Slot slot = new Slot();
            slot.deserialize(i);
            slots.add(slot);
        }
    }

    //写页头信息
    public void deserialize() {
        deserializeHeader();
        deserializeSlots();
    }

    private void insertSlot(Slot slot) {
        int low = 0, high = slots.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            if (slot.primaryKey > slots.get(mid).primaryKey)
                low = mid + 1;
            else if (slot.primaryKey < slots.get(mid).primaryKey)
                high = mid - 1;
            else
                throw new RuntimeException("try to insert a existed slot");
        }
        slots.add(low, slot);
    }

    public class Slot {
        //两个int，占用
        int offset;

        int size;
        int primaryKey;
        Slot next;

        public Slot(int offset, int size, int primaryKey) {
            this.offset = offset;
            this.size = size;
//          this.next = next;
            this.primaryKey = primaryKey;
        }

        public Slot() {
        }

        public int getPrimaryKey() {
            return primaryKey;
        }

        public int deserialize(int slotId) {
            int offset = HEADER_SIZE + slotId * SLOT_SIZE;
            this.offset = buffer.getInt(offset);
            offset += Integer.BYTES;
            this.size = buffer.getInt(offset);
            offset += Integer.BYTES;
            this.primaryKey = buffer.getInt(offset);
            offset += Integer.BYTES;
            return offset;
        }

        public void serialize(int slotId) {
            int offset = HEADER_SIZE + slotId * SLOT_SIZE;
            buffer.putInt(offset, this.offset);
            offset += Integer.BYTES;
            buffer.putInt(offset, this.size);
            offset += Integer.BYTES;
            buffer.putInt(offset, this.primaryKey);
            offset += Integer.BYTES;

        }
    }

}
