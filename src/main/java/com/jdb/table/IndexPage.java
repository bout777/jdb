package com.jdb.table;

import com.jdb.common.DataType;
import com.jdb.common.Value;
import com.jdb.index.IndexEntry;
import com.jdb.index.SecondaryIndexEntry;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Page;

import java.nio.ByteBuffer;

import static com.jdb.common.Constants.NULL_PAGE_ID;

public class IndexPage {
    private static final int HEADER_SIZE = Integer.BYTES * 3 + Byte.BYTES;
    private static final int PAGE_TYPE_OFFSET = 0;
    private static final int PAGE_ID_OFFSET = PAGE_TYPE_OFFSET + Byte.BYTES;
    private static final int NEXT_PAGE_ID_OFFSET = PAGE_ID_OFFSET + Integer.BYTES;
    private static final int ENTRY_COUNT_OFFSET = NEXT_PAGE_ID_OFFSET + Integer.BYTES;
    private final ByteBuffer bf;
    private Page page;

    public IndexPage(int id, Page page) {
        this.page = page;
        this.bf = ByteBuffer.wrap(page.getData());
        setPageId(id);
    }

    public void init() {
        setNextPageId(NULL_PAGE_ID);
        bf.put(PAGE_TYPE_OFFSET, (byte) 1);
    }

    public int getPageId() {
        return bf.getInt(PAGE_ID_OFFSET);
    }

    public void setPageId(int pageId) {
        bf.putInt(PAGE_ID_OFFSET, pageId);
    }

    public int getNextPageId() {
        return bf.getInt(NEXT_PAGE_ID_OFFSET);
    }

    public void setNextPageId(int pageId) {
        bf.putInt(NEXT_PAGE_ID_OFFSET, pageId);
    }

    public int getEntryCount() {
        return bf.getInt(ENTRY_COUNT_OFFSET);
    }

    public void setEntryCount(int entryCount) {
        bf.putInt(ENTRY_COUNT_OFFSET, entryCount);
    }


    public int binarySearch(Value<?> key) {
//        int size = PagePointer.SIZE+Integer.BYTES;
//        int low = HEADER_SIZE, high = low+(getEntryCount()-1)*size;
//        //TODO 先这样吧，测试一下
//        int k = key.getValue(Integer.class);
//        while (low<high+size){
//            int mid = (low + high) >>> 1;
//            int m = bf.getInt(mid);
//            if (m > k)
//                high = mid - size;
//            else if (m < k)
//                low = mid + size;
//            else{
//                return m;
//            }
//        }
        //返回的是offset！！不是sequence
        int low = 0, high = getEntryCount() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            IndexEntry e = getEntry(mid);
            if (e.getKey().compareTo(key) > 0)
                high = mid - 1;
            else if (e.getKey().compareTo(key) < 0)
                low = mid + 1;
            else
                return mid;
        }
        return low - 1;

    }

    public void insert(IndexEntry entry) {
        if (entry instanceof SecondaryIndexEntry) {
            // TODO 先顺序插入，后面再改
            int count = getEntryCount();
            int offset = HEADER_SIZE + entry.getBytes() * count;

            offset = entry.getKey().serialize(bf, offset);

            RecordID p = (RecordID) entry.getValue();
            bf.putInt(offset, p.pageId);
            bf.putInt(offset + Integer.BYTES, p.slotId);

            setEntryCount(count + 1);
            page.setDirty(true);
            return;
        }
        throw new RuntimeException("wrong entry type");
    }

    public IndexEntry getEntry(int eid) {
//        Value<?> key = Value.ofInt(bf.getInt(offset));
//        offset+=key.getBytes();
//        PagePointer p = new PagePointer(bf.getInt(offset), bf.getInt(offset+Integer.BYTES));
//        return new SecondaryIndexEntry(key, p);
        int offset = HEADER_SIZE + eid * (Integer.BYTES + RecordID.SIZE);
        Value<?> key = Value.deserialize(bf, offset, DataType.INTEGER);
        offset += key.getBytes();
        RecordID p = new RecordID(bf.getInt(offset), bf.getInt(offset + Integer.BYTES));
        return new SecondaryIndexEntry(key, p);
    }

    public IndexPage split() {
        BufferPool bp = BufferPool.getInstance();
        Page newPage = bp.newPage(bp.getMaxPageId());

        IndexPage newIndexPage = new IndexPage(bp.getMaxPageId() - 1, newPage);
        newIndexPage.init();

        newIndexPage.setNextPageId(this.getNextPageId());
        this.setNextPageId(newIndexPage.getPageId());

        return newIndexPage;
    }


    public Value<?> getKey(int kno) {
        int offset = HEADER_SIZE + Integer.SIZE * kno + RecordID.SIZE * (kno + 1);
        return Value.deserialize(bf, offset, DataType.INTEGER);
    }

    public int getChild(int cno) {
        int offset = HEADER_SIZE + Integer.SIZE * cno + RecordID.SIZE * cno;
        return bf.getInt(offset);
    }


    public Value<?> getFloorKey() {
        return Value.deserialize(bf, HEADER_SIZE, DataType.INTEGER);
    }

    //    private IndexEntry deserializeEntry(int offset){
//
//    }
    private void serializeEntry(IndexEntry entry, int offset) {

    }
}
