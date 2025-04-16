package com.jdb.table;

import com.jdb.catalog.Schema;
import com.jdb.exception.DuplicateInsertException;
import com.jdb.recovery.RecoveryManager;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Page;
import com.jdb.transaction.TransactionContext;
import com.jdb.version.VersionManager;

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

    private static final int PAGE_TYPE_OFFSET = 0;
    private static final int LSN_OFFSET = Byte.BYTES;
    private static final int PAGE_ID_OFFSET = LSN_OFFSET + Long.BYTES;
    private static final int NEXT_PAGE_ID_OFFSET = PAGE_ID_OFFSET + Long.BYTES;
    private static final int LOWER_OFFSET = NEXT_PAGE_ID_OFFSET + Long.BYTES;
    private static final int UPPER_OFFSET = LOWER_OFFSET + Integer.BYTES;
    private static final int HEADER_SIZE = UPPER_OFFSET + Integer.BYTES;
    // todo 这个页所属的表名，后续需要在构造函数中注入
    private String tableName = "test";
    // todo 这个页所属的表id，同上
    private Table table;
    private final ByteBuffer buffer;
    private Page page;

    private Schema schema = Schema.instance;

    public DataPage(Page page) {
        buffer = page.getBuffer();
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
        return buffer.getLong(LSN_OFFSET);
    }

    public void setLsn(long lsn) {
        buffer.putLong(LSN_OFFSET, lsn);
    }

    public int getNextPageId() {
        return buffer.getInt(NEXT_PAGE_ID_OFFSET);
    }

    public void setNextPageId(long nextPageNo) {
        buffer.putLong(NEXT_PAGE_ID_OFFSET, nextPageNo);
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
        try  {
            this.page.acquireWriteLock();

            //移动upper指针
            int upper= getUpper() - rowData.getSize();
            int lower = getLower();
            //写入slot
            Slot slot = new Slot(upper, rowData.getSize(), rowData.getPrimaryKey());
            insertSlot(slot);

            //更新lower,upper
            setUpper(upper);
            setLower(lower + SLOT_SIZE);

            //写入record
            rowData.serializeTo(buffer, upper);
            setDirty(true);
            PagePointer ptr = new PagePointer(getPageId(), upper);

            //写日志
            if(shouldLog) {
                byte[] image = new byte[slot.size];
                buffer.get(upper, image);
                long xid = TransactionContext.getTransaction().getXid();
                long lsn = RecoveryManager.getInstance().logInsert(xid, ptr, image);
                setLsn(lsn);
            }
            return ptr;
        }finally {
            this.page.releaseWriteLock();
        }
    }



    public PagePointer updateRecord(int offset, RowData rowData, boolean shouldLog) {
        try {
            this.page.acquireWriteLock();
            //todo 检查record是否符合schema

            rowData.serializeTo(buffer, offset);
            this.setDirty(true);

            PagePointer ptr = new PagePointer(getPageId(), offset);
            return ptr;
        }  finally {
            this.page.releaseWriteLock();
        }
    }

    /**
     * 用于日志重做
     *
     * @param image;
     * @throws DuplicateInsertException;
     */
    public void insertRecord(int offset, byte[] image) throws DuplicateInsertException {
        RowData rowData = RowData.deserialize(ByteBuffer.wrap(image), 0, schema);
        rowData.serializeTo(buffer, offset);
        Slot slot = new Slot(offset, image.length, rowData.getPrimaryKey());
        insertSlot(slot);
    }

    public long deleteRecord(int slotId) {

        try {
            this.page.acquireWriteLock();

            deleteSlot(slotId);
            setLower(getLower() - SLOT_SIZE);
        } finally {
            this.page.releaseWriteLock();
        }

        //后续加入页合并的逻辑
        return NULL_PAGE_ID;
    }


    public RowData getRecord(int slotId, Schema schema) {
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
    }


    /*
     * 为了测试索引的查找和升序插入，暂时不理会插入的有序性
     * 在测试代码中保证按照主键的升序插入*/
    public DataPage split() {
        BufferPool bp = BufferPool.getInstance();
        int fid = (int) (getPageId() >> Integer.SIZE);
        Page newPage = bp.newPage(fid);
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
        for (int i = 0; i < recordCount / 2; i++) {
            this.insertRecord(iter.next(), true, false);
        }
        for (int i = recordCount / 2; i < recordCount; i++) {
            newDataPage.insertRecord(iter.next(), true, false);
        }
        return newDataPage;
    }

    public Iterator<RowData> scanFrom(int slotId) {
        return new InternalRecordIterator(slotId);
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
            return getRecord(slotId++, schema);
        }
    }

    public void optimize() {
    }

}
