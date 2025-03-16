package com.idme.table;

import com.idme.storage.BufferPool;

public class TableScanner {
    private BufferPool bufferPool;
    private Table table;
    public TableScanner(BufferPool bufferPool, Table table){
        this.bufferPool = bufferPool;
        this.table = table;
    }

    public Record getNextRecord(PagePointer pointer){
        System.out.println(pointer);
        if(pointer.pageId==Integer.MAX_VALUE)
            return null;

        Page page = bufferPool.getPage(pointer.pageId);
        Record record = page.getRecord(pointer.slotId,table.columnList);


        if(pointer.slotId<page.getRecordCount()-1){
            pointer.slotId++;
        }else{
            pointer.pageId = page.getNextPageId();
            pointer.slotId = 0;
        }

        // 如果记录被删除，则返回下一条记录
        if(record.isDeleted()){
            return getNextRecord(pointer);
        }


        return record;

    }
}
