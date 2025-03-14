package com.idme.table;

import com.idme.storage.BufferPool;

public class Table {
    BufferPool bufferPool;
    int firstPageId = Integer.MAX_VALUE;
    public Table(BufferPool bufferPool){
        this.bufferPool = bufferPool;
    }
    public Record  getRecord(int pageId,int slotId){
        Page page = bufferPool.getPage(pageId);
        Record record = page.getRecord(slotId);
        return record;
    }

    public void insertRecord(Record record){
        //表还没有页面，创建一个
        if(firstPageId==Integer.MAX_VALUE){
            firstPageId = 0;
            bufferPool.newPage(firstPageId);
        }

        int pageId = firstPageId;
        Page page = bufferPool.getPage(pageId);

        //遍历页面，找到一个能插入的
        while (page.getFreeSpace()<record.getSize()&& page.getNextPageId()!=Integer.MAX_VALUE){
            pageId++;
            page = bufferPool.getPage(pageId);
        }

        //如果找不到，创建一个新页面
        if(page.getFreeSpace()<record.getSize()&& page.getNextPageId()==Integer.MAX_VALUE){
            pageId++;
            page.setNextPageId(pageId);
            page = bufferPool.newPage(pageId);
        }

        //插入record
        page.insertRecord(record);
    }

    public void deleteRecord(int pageId,int slotId){
        Page page = bufferPool.getPage(pageId);
        page.deleteRecord(slotId);
    }
}
