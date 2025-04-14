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

    public RowData getNextRecord(PagePointer pointer) {
        if (pointer.pid == NULL_PAGE_ID)
            return null;

        DataPage dataPage = new DataPage(bufferPool.getPage(pointer.pid));
        RowData rowData = dataPage.getRecord(pointer.offset, table.schema);


        if (pointer.offset < dataPage.getRecordCount() - 1) {
            pointer.offset++;
        } else {
            pointer.pid = dataPage.getNextPageId();
            pointer.offset = 0;
        }

        // 如果记录被删除，则返回下一条记录
        if (rowData.isDeleted()) {
            return getNextRecord(pointer);
        }


        return rowData;

    }
}
