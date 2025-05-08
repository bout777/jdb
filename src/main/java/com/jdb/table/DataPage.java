package com.jdb.table;

import com.jdb.catalog.Schema;
import com.jdb.common.value.Value;
import com.jdb.exception.DuplicateInsertException;
import com.jdb.recovery.RecoveryManager;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Page;
import com.jdb.storage.PageType;
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

    public static final int PAGE_TYPE_OFFSET = 0;
    public static final int LSN_OFFSET = Byte.BYTES;
    public static final int PAGE_ID_OFFSET = LSN_OFFSET + Long.BYTES;
    public static final int NEXT_PAGE_ID_OFFSET = PAGE_ID_OFFSET + Long.BYTES;
    public static final int LOWER_OFFSET = NEXT_PAGE_ID_OFFSET + Long.BYTES;
    public static final int UPPER_OFFSET = LOWER_OFFSET + Integer.BYTES;
    public static final int HEADER_SIZE = UPPER_OFFSET + Integer.BYTES;
    private final ByteBuffer buffer;
    private final Page page;
    private final BufferPool bufferPool;
    private final RecoveryManager recoveryManager;
    private final Schema schema;
    private Table table;

    public DataPage(Page page, BufferPool bp, RecoveryManager rm, Schema schema) {
        buffer = page.getBuffer();
        setPageId(page.pid);
        this.page = page;
        this.bufferPool = bp;
        this.recoveryManager = rm;
        this.schema = schema;
    }

    public void init() {
        //log
        long xid = TransactionContext.getTransaction().getXid();
        long lsn = recoveryManager.logDataPageInit(xid, page.pid);
        page.setLsn(lsn);
        //set byte
        doInit();
    }

    public void doInit() {
        setNextPageId(NULL_PAGE_ID, false);
        setLower(HEADER_SIZE);
        setUpper(PAGE_SIZE);
        buffer.put(0, PageType.DATA_PAGE);
    }

//    public long getLsn() {
//        return buffer.getLong(LSN_OFFSET);
//    }

    public void setPageLsn(long lsn) {
        buffer.putLong(LSN_OFFSET, lsn);
    }

    public long getNextPageId() {
        return buffer.getLong(NEXT_PAGE_ID_OFFSET);
    }

    public void setNextPageId(long nextPageId, boolean shouldLog) {
        if (shouldLog) {
            long xid = TransactionContext.getTransaction().getXid();
            long lsn = recoveryManager.logPageLink(xid, page.pid, getNextPageId(), nextPageId);
            setPageLsn(lsn);
        }
        buffer.putLong(NEXT_PAGE_ID_OFFSET, nextPageId);
    }

    public long getPageId() {
        return buffer.getLong(PAGE_ID_OFFSET);
    }

    public void setPageId(long pid) {
        buffer.putLong(PAGE_ID_OFFSET, pid);
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

    public int getFreeSpace() {
        return getUpper() - getLower();
    }

    public int getHeaderSize() {
        return HEADER_SIZE;
    }

    public PagePointer insertRecord(RowData rowData, boolean shouldLog, boolean shouldPushVersion) {
        try {
            this.page.acquireWriteLock();

            //移动upper指针
            int upper = getUpper() - rowData.size();
            int lower = getLower();
            //写入slot
            Slot slot = new Slot(upper, rowData.size());
            int sid = insertSlot(slot, rowData.getPrimaryKey());

            //更新lower,upper
            setUpper(upper);
            setLower(lower + SLOT_SIZE);

            //写入record
            rowData.serialize(buffer, upper);
            setDirty(true);
            PagePointer ptr = new PagePointer(getPageId(), sid);

            //写日志
            if (shouldLog) {
                byte[] image = new byte[slot.size];
                buffer.get(upper, image);
                long xid = TransactionContext.getTransaction().getXid();
                //fixme 构造器注入
                long lsn = recoveryManager.logInsert(xid, ptr, image);
                setPageLsn(lsn);
            }
            return ptr;
        } finally {
            this.page.releaseWriteLock();
        }
    }

    public long deleteRecord(Value<?> key, boolean shouldLog) {
        try {
            this.page.acquireWriteLock();

            int sid = binarySearch(key);
            if (sid < 0)
                throw new NoSuchElementException("key not found");

            //写日志
            if (shouldLog) {
                Slot slot = getSlot(sid);
                PagePointer ptr = new PagePointer(getPageId(), sid);
                byte[] image = Arrays.copyOfRange(buffer.array(), slot.offset, slot.offset + slot.size);
                long xid = TransactionContext.getTransaction().getXid();
                long lsn = recoveryManager.logDelete(xid, ptr, image);
                page.setLsn(lsn);
            }

            //删除
            deleteRecord(sid);
        } finally {
            this.page.releaseWriteLock();
        }
        return NULL_PAGE_ID;
    }


//    public PagePointer updateRecord(int offset, RowData rowData, boolean shouldLog) {
//        try {
//            this.page.acquireWriteLock();
//            //todo 检查record是否符合schema
//
//            rowData.serializeTo(buffer, offset);
//            this.setDirty(true);
//
//            PagePointer ptr = new PagePointer(getPageId(), offset);
//            return ptr;
//        }  finally {
//            this.page.releaseWriteLock();
//        }
//    }

    /**
     * 用于日志重做
     *
     * @param image;
     * @throws DuplicateInsertException;
     */
    public void insertRecord(int slotId, byte[] image) throws DuplicateInsertException {
        RowData rowData = RowData.deserialize(ByteBuffer.wrap(image), 0, schema);
        int upper = getUpper() - rowData.size();
        rowData.serialize(buffer, upper);
        Slot slot = new Slot(upper, image.length);

        //todo 后续改成根据sid插入,实现o(n)
        insertSlot(slot, rowData.getPrimaryKey());
    }

    public void deleteRecord(int offset, byte[] image) {
        RowData rowData = RowData.deserialize(ByteBuffer.wrap(image), 0, schema);
    }


    public long deleteRecord(int slotId) {
        //todo lock

        Slot slot = getSlot(slotId);
        var ptr = new PagePointer(getPageId(), slotId);
        byte[] image = Arrays.copyOfRange(buffer.array(), slot.offset, slot.offset + slot.size);

        deleteSlot(slotId);
        setLower(getLower() - SLOT_SIZE);


        //后续加入页合并的逻辑
        return NULL_PAGE_ID;
    }


    public RowData getRecord(int slotId) {
        try {
            this.page.acquireReadLock();
            if (slotId >= getRecordCount()) {
                throw new NoSuchElementException();
            }
            Slot slot = getSlot(slotId);
            //读取最新记录
            return RowData.deserialize(buffer, slot.offset, schema);
        } finally {
            this.page.releaseReadLock();
        }
    }

    private int insertSlot(Slot slot, Value<?> key) {
        int low = binarySearch(key);
        if (low >= 0)
            throw new DuplicateInsertException("try to insert a existed slot");
        low = -low - 1;
        byte[] data = page.getData();
        //本地方法移动byte数组，腾出插入位置
        int offset = HEADER_SIZE + low * SLOT_SIZE;
        System.arraycopy(data, offset, data, offset + SLOT_SIZE, (getRecordCount() - low) * SLOT_SIZE);
        slot.serialize(offset, buffer);

        //返回插入的位置，用于日志
        return low;
    }

    private Value<?> getPrimaryKey(Slot slot) {
        var type = schema.columns().get(0).getType();
        Value<?> key = Value.deserialize(buffer, slot.offset, type);
        return key;
    }

    private void deleteSlot(int slotId) {
        int offset = HEADER_SIZE + slotId * SLOT_SIZE;
        byte[] data = page.getData();
        System.arraycopy(data, offset + SLOT_SIZE, data, offset, (getRecordCount() - slotId) * SLOT_SIZE);
    }


    /*
     * 为了测试索引的查找和升序插入，暂时不理会插入的有序性
     * 在测试代码中保证按照主键的升序插入*/
    public DataPage split() {
        int fid = (int) (getPageId() >> Integer.SIZE);
        Page newPage = bufferPool.newPage(fid, true);
        //创建一个当前页的镜像页
        Page image = new Page(Arrays.copyOf(this.page.getData(), this.page.getData().length));
        DataPage imageDataPage = new DataPage(image, bufferPool, recoveryManager, schema);

        //初始化当前页和新页
        DataPage newDataPage = new DataPage(newPage, bufferPool, recoveryManager, schema);
        newDataPage.init();
        newDataPage.setNextPageId(this.getNextPageId(), true);
        this.init();
        this.setNextPageId(newDataPage.getPageId(), true);

        //读取镜像页的数据，分别插入当前页和新页
        int recordCount = imageDataPage.getRecordCount();
        var iter = imageDataPage.scanFrom(0);
        //fixme 如果shuoldLog为ture,过不了回滚测试,如果为false,过不了崩溃恢复测试
        for (int i = 0; i < recordCount / 2; i++) {
            this.insertRecord(iter.next(), true, false);
        }
        for (int i = recordCount / 2; i < recordCount; i++) {
            newDataPage.insertRecord(iter.next(), true, false);
        }
        return newDataPage;
    }

    public int binarySearch(Value<?> key) {
        int low = 0, high = getRecordCount() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            var midKey = getRecord(mid).getPrimaryKey();
            if (key.compareTo(midKey) < 0)
                high = mid - 1;
            else if (key.compareTo(midKey) > 0)
                low = mid + 1;
            else {
                return mid;
            }
        }
        //返回插入位置
        return -low - 1;
    }

    public Iterator<RowData> scanFrom(int slotId) {
        return new InternalRecordIterator(slotId);
    }

    public void optimize() {
    }

    class InternalRecordIterator implements Iterator<RowData> {
        int slotId;

        InternalRecordIterator(int slotId) {
            this.slotId = slotId;
        }

        @Override
        public boolean hasNext() {
            return slotId < getRecordCount();
        }

        @Override
        public RowData next() {
            if (!hasNext())
                throw new NoSuchElementException();
            return getRecord(slotId++);
        }
    }

}
