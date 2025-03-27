package com.jdb.table;

import com.jdb.catalog.ColumnList;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Page;

import static com.jdb.common.Constants.NULL_PAGE_ID;
import static com.jdb.common.Constants.SLOT_SIZE;

public class Table {
    BufferPool bufferPool;
    int firstPageId = NULL_PAGE_ID;
    ColumnList columnList;

    public Table(BufferPool bufferPool, ColumnList columnList) {
        this.bufferPool = bufferPool;
        this.columnList = columnList;
    }

    // TODO 把get相关的逻辑弄好，测试，下一步才可以对接索引
    public Record getRecord(int pageId, int slotId) {
        Page page = bufferPool.getPage(pageId);
        DataPage dataPage = new DataPage(pageId, page);
        Record record = dataPage.getRecord(slotId, columnList);
        return record;
    }

    public void insertRecord(Record record) {
        //表还没有页面，创建一个
        if (firstPageId == NULL_PAGE_ID) {
            firstPageId = 0;
            bufferPool.newPage(firstPageId);
            DataPage dataPage = new DataPage(firstPageId, bufferPool.getPage(firstPageId));
            dataPage.init();
        }

        int pageId = firstPageId;

        DataPage dataPage = new DataPage(pageId, bufferPool.getPage(pageId));

        //遍历页面，找到一个能插入的
        while (dataPage.getFreeSpace() < record.getSize() + SLOT_SIZE && dataPage.getNextPageId() != NULL_PAGE_ID) {
            pageId++;
            dataPage = new DataPage(pageId, bufferPool.getPage(pageId));
        }
        //如果找不到，创建一个新页面
        if (dataPage.getFreeSpace() < record.getSize() + SLOT_SIZE && dataPage.getNextPageId() == NULL_PAGE_ID) {
            pageId++;
            dataPage.setNextPageId(pageId);
            Page page = bufferPool.newPage(pageId);
            dataPage = new DataPage(pageId, page);
            dataPage.init();
        }

        //插入record
        dataPage.insertRecord(record);
    }

//    public void deleteRecord(PagePointer p) {
//        DataPage dataPage = new DataPage(p.pageId, bufferPool.getPage(p.pageId));
//        dataPage.deleteRecord(p.slotId);
//    }
}
