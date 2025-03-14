package com.idme.table;

import com.idme.storage.BufferPool;

public class TableScanner {
    private BufferPool bufferPool;
    private Table table;
    public TableScanner(BufferPool bufferPool, Table table){
        this.bufferPool = bufferPool;
        this.table = table;
    }

    public void getNextRecord(){

    }
}
