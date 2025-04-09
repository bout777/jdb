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
    //[type] [lsn] [pid] [nextPid] [entryCount] [entry]...
    private static final int PAGE_TYPE_OFFSET = 0;
    private static final int LSN_OFFSET = Byte.BYTES;
    private static final int PAGE_ID_OFFSET = LSN_OFFSET + Long.BYTES;
    private static final int NEXT_PAGE_ID_OFFSET = PAGE_ID_OFFSET + Long.BYTES;
    private static final int ENTRY_COUNT_OFFSET = NEXT_PAGE_ID_OFFSET + Long.BYTES;
    private static final int HEADER_SIZE = ENTRY_COUNT_OFFSET + Integer.BYTES;
    private final ByteBuffer bf;
    private Page page;
    private String tableName = "test";

    public IndexPage(long pid, Page page) {
        this.page = page;
        this.bf = ByteBuffer.wrap(page.getData());
        setPageId(pid);
    }

    public void init() {
        setNextPageId(NULL_PAGE_ID);
        bf.put(PAGE_TYPE_OFFSET, (byte) 1);
    }

    public long getPageId() {
        return bf.getLong(PAGE_ID_OFFSET);
    }

    public void setPageId(long pid) {
        bf.putLong(PAGE_ID_OFFSET, pid);
    }

    public long getNextPageId() {
        return bf.getLong(NEXT_PAGE_ID_OFFSET);
    }

    public void setNextPageId(long pid) {
        bf.putLong(NEXT_PAGE_ID_OFFSET, pid);
    }

    public int getEntryCount() {
        return bf.getInt(ENTRY_COUNT_OFFSET);
    }

    public void setEntryCount(int entryCount) {
        bf.putInt(ENTRY_COUNT_OFFSET, entryCount);
    }


    // return the index of max floor entry of key
    public int binarySearch(Value<?> key) {
        int low = 0, high = getEntryCount() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            IndexEntry e = getEntry(mid);
            if (e.getKey().compareTo(key) <= 0) {
                low = mid + 1;
            } else if (e.getKey().compareTo(key) > 0) {
                high = mid - 1;
            }
        }
        return low - 1;
    }

    public void insert(IndexEntry entry) {
        if (entry instanceof SecondaryIndexEntry) {
            int eid = binarySearch(entry.getKey()) + 1;
            int offset = HEADER_SIZE + entry.getBytes() * eid;

            offset = entry.getKey().serialize(bf, offset);

            RecordID p = (RecordID) entry.getValue();
            bf.putLong(offset, p.pid);
            offset += Long.BYTES;
            bf.putInt(offset, p.slotId);

            setEntryCount(getEntryCount() + 1);
            page.setDirty(true);
            return;
        }
        throw new UnsupportedOperationException("wrong entry type");
    }

    public IndexEntry getEntry(int eid) {
        int offset = HEADER_SIZE + eid * (Integer.BYTES + RecordID.SIZE);
        Value<?> key = Value.deserialize(bf, offset, DataType.INTEGER);
        offset += key.getBytes();
        RecordID p = RecordID.deserialize(bf, offset);
        return new SecondaryIndexEntry(key, p);
    }

    public IndexPage split() {
        BufferPool bp = BufferPool.getInstance();
        Page newPage = bp.newPage(tableName);

        IndexPage newIndexPage = new IndexPage(newPage.pid, newPage);
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
