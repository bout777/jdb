package com.jdb.table;

import com.jdb.storage.BufferPool;

import static com.jdb.common.Constants.NULL_PAGE_ID;

public class TableScanner {
    private final BufferPool bufferPool;
    private final Table table;

    public TableScanner(BufferPool bufferPool, Table table) {
        this.bufferPool = bufferPool;
        this.table = table;
    }

    public Record getNextRecord(PagePointer pointer) {
        System.out.println(pointer);
        if (pointer.pageId == NULL_PAGE_ID)
            return null;

        DataPage dataPage = new DataPage(pointer.pageId, bufferPool.getPage(pointer.pageId));
        Record record = dataPage.getRecord(pointer.slotId, table.columnList);


        if (pointer.slotId < dataPage.getRecordCount() - 1) {
            pointer.slotId++;
        } else {
            pointer.pageId = dataPage.getNextPageId();
            pointer.slotId = 0;
        }

        // 如果记录被删除，则返回下一条记录
        if (record.isDeleted()) {
            return getNextRecord(pointer);
        }


        return record;

    }
}
