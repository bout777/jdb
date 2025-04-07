package com.jdb.table;

import com.jdb.catalog.ColumnList;
import com.jdb.exception.DuplicateInsertException;
import com.jdb.recovery.RecoveryManager;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Page;
import com.jdb.transaction.TransactionContext;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.jdb.common.Constants.*;

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

    private static final int HEADER_SIZE = Integer.BYTES * 4 + Byte.BYTES;
    private static final int PAGE_TYPE_OFFSET = 0;
    private static final int PAGE_ID_OFFSET = Byte.BYTES;
    private static final int NEXT_PAGE_ID_OFFSET = PAGE_ID_OFFSET + Integer.BYTES;
    private static final int LOWER_OFFSET = NEXT_PAGE_ID_OFFSET + Integer.BYTES;
    private static final int UPPER_OFFSET = LOWER_OFFSET + Integer.BYTES;
    // todo 这个页所属的表名，后续需要在构造函数中注入
    private String tableName = "test";
    // todo 这个页所属的表id，同上
    private int fid = 1234;

    private final ByteBuffer buffer;
    private Page page;

    private ColumnList columnList = ColumnList.instance;

    public DataPage(Page page) {
        buffer = ByteBuffer.wrap(page.getData());
        setPageId(page.pid);
        this.page = page;
    }

    public void init() {
        setNextPageId(NULL_PAGE_ID);
        setLower(HEADER_SIZE);
        setUpper(PAGE_SIZE);
        byte b = 2;
        buffer.put(0, b);
    }

    public long getLsn() {
        return 0;
    }

    public int getNextPageId() {
        return buffer.getInt(NEXT_PAGE_ID_OFFSET);
    }

    public void setNextPageId(int nextPageId) {
        buffer.putInt(NEXT_PAGE_ID_OFFSET, nextPageId);
    }

    public int getPageId() {
        return buffer.getInt(PAGE_ID_OFFSET);
    }

    public void setPageId(int pageId) {
        buffer.putInt(PAGE_ID_OFFSET, pageId);
    }

    public int getUpper() {
        return buffer.getInt(UPPER_OFFSET);
    }

    public void setUpper(int upper) {
        buffer.putInt(UPPER_OFFSET, upper);
    }

    public int getLower() {
        return buffer.getInt(LOWER_OFFSET);
    }

    public void setLower(int lower) {
        buffer.putInt(LOWER_OFFSET, lower);
    }


    public int getRecordCount() {
        return (getLower() - HEADER_SIZE) / SLOT_SIZE;
    }

    public boolean isDirty() {
        return page.isDirty();
    }

    public void setDirty(boolean dirty) {
        page.setDirty(dirty);
    }

    public Slot getSlot(int slotId) {
        int offset = HEADER_SIZE + slotId * SLOT_SIZE;
        return Slot.deserialize(offset, buffer);
    }

    private void putSlot(Slot slot) {
        int offset = HEADER_SIZE + slot.offset * SLOT_SIZE;
        slot.serialize(offset, buffer);
    }
//    public List<PagePointer> getPointers() {
//        List<PagePointer> pointers = new ArrayList<>();
//        int pageId = getPageId();
//        for (int i = 0; i < slots.size(); i++) {
//            PagePointer pointer = new PagePointer(pageId, i);
//            pointers.add(pointer);
//        }
//
//        return pointers;
//    }

//    public List<Slot> getSlots() {
//        return slots;
//    }

    public int getFreeSpace() {
        return getUpper() - getLower();
    }

    public void insertRecord(Record record) {

        //移动upper指针
        int upper = getUpper() - record.getSize();

        //写入slot
        Slot slot = new Slot(upper, record.getSize(), record.getPrimaryKey());
        int slotId= insertSlot(slot);

        //更新lower,upper
        setUpper(upper);
        setLower(getLower() + SLOT_SIZE);

        //写入record
        record.serializeTo(buffer, upper);
        this.setDirty(true);

        //写日志
        byte[] image = new byte[slot.size];
        buffer.get(upper, image);
        long xid = TransactionContext.getTransaction().getXid();
        RecoveryManager.getInstance().logInsert(xid,this.fid ,new RecordID(getPageId(), slotId),image);
    }

    /**
     * 用于日志重做
     *
     * @param image;
     * @throws DuplicateInsertException;
     */
    public void insertRecord(int slotId, byte[] image) throws DuplicateInsertException{
        Record record = new Record();
        record.deserializeFrom(ByteBuffer.wrap(image), 0,columnList);
        insertRecord(record);
    }

    public void deleteRecord(int slotId) {
        Slot slot = getSlot(slotId);
        deleteSlot(slotId);

        Record record = new Record();
        record.deserializeHeader(buffer, slot.offset);
        record.setDeleted(true);
        record.serializeHeader(buffer, slot.offset);
    }

//    public void deleteRecord(int slotId) {
//        Slot slot = slots.get(slotId);
//        Record record = new Record();
//        record.deserializeHeader(buffer, slot.offset);
//        record.setDeleted(true);
//        record.serializeHeader(buffer, slot.offset);
//    }


    public Record getRecord(int slotId, ColumnList columnList) {
        Record record = new Record();
        Slot slot = getSlot(slotId);
        record.size = slot.size;
        record.deserializeFrom(buffer, slot.offset, columnList);
        return record;
    }

//    public void serialize() {
//        serializeHeader();
//        serializeSlots();
//    }

//    private void serializeHeader() {
//        int offset = 0;
//        buffer.putInt(offset, pageId);
//
//        offset += Integer.BYTES;
//        buffer.putInt(offset, nextPageId);
//
//        offset += Integer.BYTES;
//        buffer.putInt(offset, lower);
//
//        offset += Integer.BYTES;
//        buffer.putInt(offset, upper);
//
//    }

//    private void deserializeHeader() {
//        int offset = 0;
////        pageId = buffer.getInt(offset);
//        offset += Integer.BYTES;
//
//        nextPageId = buffer.getInt(offset);
//        offset += Integer.BYTES;
//
//        lower = buffer.getInt(offset);
//        offset += Integer.BYTES;
//
//        upper = buffer.getInt(offset);
//        offset += Integer.BYTES;
//    }

//    private void serializeSlots() {
//        int n = getRecordCount();
//        int offset = HEADER_SIZE;
//        for (int i = 0; i < n; i++) {
//            slots.get(i).serialize(offset, buffer);
//            offset += SLOT_SIZE;
//        }
//    }
//
//    private void deserializeSlots() {
//        if (!slots.isEmpty())
//            throw new RuntimeException("slots is not empty");
//
//        int n = getRecordCount();
//        int offset = HEADER_SIZE;
//        for (int i = 0; i < n; i++) {
//            Slot slot = Slot.deserialize(offset, buffer);
//            slots.add(slot);
//            offset+=SLOT_SIZE;
//        }
//    }

    //写页头信息
//    public void deserialize() {
//        deserializeHeader();
//        deserializeSlots();
//    }

    private int insertSlot(Slot slot) {
        int low = 0, high = getRecordCount() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            Slot midSlot = getSlot(mid);
            if (slot.primaryKey > midSlot.primaryKey)
                low = mid + 1;
            else if (slot.primaryKey < midSlot.primaryKey)
                high = mid - 1;
            else
                throw new DuplicateInsertException("try to insert a existed slot");
        }
        byte[] data = page.getData();
        //本地方法移动byte数组，腾出插入位置
        int offset = HEADER_SIZE + low * SLOT_SIZE;
        System.arraycopy(data, offset, data, offset + SLOT_SIZE, (getRecordCount() - low) * SLOT_SIZE);
        slot.serialize(offset, buffer);

        //返回插入的位置，用于日志
        return low;
    }

    private void deleteSlot(int slotId) {
        int offset = HEADER_SIZE + slotId * SLOT_SIZE;
        byte[] data = page.getData();
        System.arraycopy(data, offset + SLOT_SIZE, data, offset, (getRecordCount() - slotId - 1) * SLOT_SIZE);
        setLower(getLower() - SLOT_SIZE);
    }


    /*
     * 为了测试索引的查找和升序插入，暂时不理会插入的有序性
     * 在测试代码中保证按照主键的升序插入*/
    public DataPage split() {
        BufferPool bp = BufferPool.getInstance();
        Page newPage = bp.newPage(tableName);
        //创建一个当前页的镜像页
        Page image = new Page(Arrays.copyOf(this.page.getData(), this.page.getData().length));
        DataPage imageDataPage = new DataPage(image);

        //初始化当前页和新页
        this.init();
        DataPage newDataPage = new DataPage(newPage);
        newDataPage.init();

        newDataPage.setNextPageId(this.getNextPageId());
        this.setNextPageId(newDataPage.getPageId());

        //读取镜像页的数据，分别插入当前页和新页
        int recordCount = imageDataPage.getRecordCount();
        var iter = imageDataPage.scanFrom(0);
        for (int i = 0; i < recordCount/2; i++) {
            this.insertRecord(iter.next());
        }
        for(int i = recordCount/2; i < recordCount; i++){
            newDataPage.insertRecord(iter.next());
        }
        return newDataPage;
    }

    public Iterator<Record> scanFrom(int slotId) {
        return new InternalRecordIterator(slotId);
    }


    class InternalRecordIterator implements Iterator<Record> {
        int slotId;

        InternalRecordIterator(int slotId) {
            this.slotId = slotId;
        }

        @Override
        public boolean hasNext() {
            return slotId < getRecordCount();
        }

        @Override
        public Record next() {
            if (!hasNext())
                throw new NoSuchElementException();
            return getRecord(slotId++, columnList);
        }
    }

    public void optimize() {
    }

}
