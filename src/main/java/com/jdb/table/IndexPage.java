package com.jdb.table;

import com.jdb.common.DataType;
import com.jdb.common.PageHelper;
import com.jdb.common.Value;
import com.jdb.index.IndexEntry;
import com.jdb.index.SecondaryIndexEntry;
import com.jdb.recovery.RecoveryManager;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Page;
import com.jdb.storage.PageType;
import com.jdb.transaction.TransactionContext;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.jdb.common.Constants.NULL_PAGE_ID;

public class IndexPage {
    //[type] [lsn] [pid] [nextPid] [entryCount] [entry]...
    public static final int PAGE_TYPE_OFFSET = 0;
    private static final int LSN_OFFSET = Byte.BYTES;
    private static final int PAGE_ID_OFFSET = LSN_OFFSET + Long.BYTES;
    private static final int NEXT_PAGE_ID_OFFSET = PAGE_ID_OFFSET + Long.BYTES;
    private static final int KEY_COUNT_OFFSET = NEXT_PAGE_ID_OFFSET + Long.BYTES;
    private static final int HEADER_SIZE = KEY_COUNT_OFFSET + Integer.BYTES;

    private static final int KEY_SIZE = Integer.BYTES;
    private static final int CHILD_SIZE = Integer.BYTES;

    private final ByteBuffer bf;
    private final RecoveryManager recoveryManager;
    private Page page;
    private BufferPool bufferPool;


    public IndexPage(Page page, BufferPool bp, RecoveryManager rm) {
        this.page = page;
        this.bf = ByteBuffer.wrap(page.getData());
        this.bufferPool = bp;
        this.recoveryManager = rm;
        setPageId(page.pid);
    }

    public void init() {
        long xid = TransactionContext.getTransaction().getXid();
        long lsn = recoveryManager.logIndexPageInit(xid, page.pid);
        page.setLsn(lsn);
        doInit();
    }

    public void doInit() {
        setNextPageId(NULL_PAGE_ID);
        bf.put(PAGE_TYPE_OFFSET, PageType.INDEX_PAGE);
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

    public int getKeyCount() {
        return bf.getInt(KEY_COUNT_OFFSET);
    }

    public void setKeyCount(int count) {
        bf.putInt(KEY_COUNT_OFFSET, count);
    }


    // return the com.jdb.index of max floor entry of key
    public int binarySearch(Value<?> key) {
        int low = 0, high = getKeyCount() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            Value<?> m = getKey(mid);
            if (m.compareTo(key) <= 0) {
                low = mid + 1;
            } else if (m.compareTo(key) > 0) {
                high = mid - 1;
            }
        }
        return low - 1;
    }

//    public void insert(IndexEntry entry) {
//        if (entry instanceof SecondaryIndexEntry) {
//            int eid = binarySearch(entry.getKey()) + 1;
//            int offset = HEADER_SIZE + entry.getBytes() * eid;
//
//            offset = entry.getKey().serialize(bf, offset);
//
//            RecordID p = (RecordID) entry.getValue();
//            bf.putLong(offset, p.pid);
//            offset += Long.BYTES;
//            bf.putInt(offset, p.offset);
//
//            setKeyCount(getKeyCount() + 1);
//            page.setDirty(true);
//            return;
//        }
//        throw new UnsupportedOperationException("wrong entry type");
//    }

    public long insert(Value<?> key, long pid, boolean shouldLog) {
        this.page.acquireWriteLock();
        //找到插入位置
        int idx = binarySearch(key) + 1;
        int offset = HEADER_SIZE + KEY_SIZE * idx + CHILD_SIZE * (idx + 1);

        //腾出空间
        int len = getKeyCount() * (KEY_SIZE + CHILD_SIZE);
        System.arraycopy(bf.array(), offset, bf.array(), offset + KEY_SIZE + CHILD_SIZE, len);

        //写入
        int pno = PageHelper.getPno(pid);
        offset = key.serialize(bf, offset);
        bf.putInt(offset, pno);

        setKeyCount(getKeyCount() + 1);

        if (shouldLog) {
            long xid = TransactionContext.getTransaction().getXid();
            long lsn = recoveryManager.logIndexInsert(xid, page.pid, key, pid);
            page.setLsn(lsn);
        }

        this.page.releaseWriteLock();
        //todo 添加分裂逻辑
        return -1L;
    }

    public void addChild(int cno, long pid) {
//        this.page.acquireWriteLock();
//        int offset = HEADER_SIZE + KEY_SIZE * cno + CHILD_SIZE * cno;
//        System.arraycopy(bf.array(), offset, bf.array(), offset + CHILD_SIZE, CHILD_SIZE * (cno + 1));
//        int pno = PageHelper.getPno(pid);
//        bf.putInt(offset, pno);
//        this.page.releaseWriteLock();
        this.page.acquireWriteLock();
        try {
            int offset = HEADER_SIZE + KEY_SIZE * cno + CHILD_SIZE * cno;
            // Capture the 4 bytes before change
            byte[] before = new byte[CHILD_SIZE];
            System.arraycopy(bf.array(), offset, before, 0, CHILD_SIZE);

            // Shift existing child entries to make room
            System.arraycopy(
                    bf.array(),
                    offset,
                    bf.array(),
                    offset + CHILD_SIZE,
                    CHILD_SIZE * (cno + 1)
            );

            // Compute the page number to insert
            int pno = PageHelper.getPno(pid);
            // Write the new child pointer
            bf.putInt(offset, pno);

            // Prepare the after-image of the 4 bytes
            byte[] after = ByteBuffer.allocate(CHILD_SIZE)
                    .order(ByteOrder.BIG_ENDIAN)
                    .putInt(pno)
                    .array();

            // Log the update
            long xid = TransactionContext.getTransaction().getXid();
            recoveryManager.logGeneralPageUpdate(xid, page.pid, (short) offset, before, after);
        } finally {
            this.page.releaseWriteLock();
        }
    }

    public int getChild(int cno) {
        int offset = HEADER_SIZE + KEY_SIZE * cno + CHILD_SIZE * cno;
        return bf.getInt(offset);
    }

    public void addKey(int kno, Value<?> key) {
        int offset = HEADER_SIZE + KEY_SIZE * kno + CHILD_SIZE * (kno + 1);
        key.serialize(bf, offset);
    }

    public Value<?> getKey(int kno) {
        int offset = HEADER_SIZE + KEY_SIZE * kno + CHILD_SIZE * (kno + 1);
        return Value.deserialize(bf, offset, DataType.INTEGER);
    }

    public IndexEntry getEntry(int eid) {
        int offset = HEADER_SIZE + eid * (Integer.BYTES + PagePointer.SIZE);
        Value<?> key = Value.deserialize(bf, offset, DataType.INTEGER);
        offset += key.getBytes();
        PagePointer p = PagePointer.deserialize(bf, offset);
        return new SecondaryIndexEntry(key, p);
    }

    public IndexPage split() {
        int fid = PageHelper.getPno(page.pid);
        Page newPage = bufferPool.newPage(fid, true);
        IndexPage newIndexPage = new IndexPage(newPage, bufferPool, recoveryManager);
        newIndexPage.init();

        newIndexPage.setNextPageId(this.getNextPageId());
        this.setNextPageId(newIndexPage.getPageId());

        return newIndexPage;
    }


    public Value<?> getFloorKey() {
        return Value.deserialize(bf, HEADER_SIZE + CHILD_SIZE, DataType.INTEGER);
    }

    //    private IndexEntry deserializeEntry(int offset){
//
//    }
    private void serializeEntry(IndexEntry entry, int offset) {

    }
}
