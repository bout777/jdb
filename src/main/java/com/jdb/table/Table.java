package com.jdb.table;

import com.jdb.catalog.Schema;
import com.jdb.index.BPTree;
import com.jdb.index.Index;
import com.jdb.index.IndexMetaData;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Page;

import static com.jdb.common.Constants.NULL_PAGE_ID;
import static com.jdb.common.Constants.SLOT_SIZE;

public class Table {
    BufferPool bufferPool;
    long firstPageId = NULL_PAGE_ID;
    Index clusterIndex;

    public Schema getSchema() {
        return schema;
    }

    Schema schema;
    String tableName = "test";
    public Table(String name, Schema schema) {
        this.bufferPool = BufferPool.getInstance();
        this.schema = schema;
        this.tableName = name;

        IndexMetaData metaData = new IndexMetaData(getTableName(),schema.columns().get(0),"test",schema);
        clusterIndex = new BPTree(metaData);
    }


    public Index getClusterIndex() {
        return clusterIndex;
    }


    public String getTableName() {

        return tableName;
    }

    // TODO 把get相关的逻辑弄好，测试，下一步才可以对接索引
    public Record getRecord(RecordID rid) {
        Page page = bufferPool.getPage(rid.pid);
        DataPage dataPage = new DataPage(page);
        Record record = dataPage.getRecord(rid.offset, schema);
        return record;
    }

    //需要改成走索引插入
    public void insertRecord(Record record) {
        //表还没有页面，创建一个
        if (firstPageId == NULL_PAGE_ID) {
            firstPageId = 0;
            bufferPool.newPage(tableName);
            DataPage dataPage = new DataPage(bufferPool.getPage(firstPageId));
            dataPage.init();
        }

        long pid = firstPageId;

        DataPage dataPage = new DataPage(bufferPool.getPage(pid));

        //遍历页面，找到一个能插入的
        while (dataPage.getFreeSpace() < record.getSize() + SLOT_SIZE && dataPage.getNextPageId() != NULL_PAGE_ID) {
            pid++;
            dataPage = new DataPage(bufferPool.getPage(pid));
        }
        //如果找不到，创建一个新页面
        if (dataPage.getFreeSpace() < record.getSize() + SLOT_SIZE && dataPage.getNextPageId() == NULL_PAGE_ID) {
            pid++;
            dataPage.setNextPageId(pid);
            Page page = bufferPool.newPage(tableName);
            dataPage = new DataPage(page);
            dataPage.init();
        }

        //插入record
        dataPage.insertRecord(record, true, true);
    }

//    public void deleteRecord(PagePointer p) {
//        DataPage dataPage = new DataPage(p.pageId, bufferPool.getPage(p.pageId));
//        dataPage.deleteRecord(p.slotId);
//    }
}
