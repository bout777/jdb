package com.jdb.table;

import com.jdb.catalog.Column;
import com.jdb.catalog.Schema;
import com.jdb.common.DataType;
import com.jdb.recovery.LogManager;
import com.jdb.recovery.RecoveryManager;
import com.jdb.storage.BufferPool;

public class TableManager {
    //for test
    private static TableManager instance;
    public static TableManager getInstance() {
        if(instance == null){
            instance = new TableManager(BufferPool.getInstance());
        }
        return instance;
    }



    BufferPool bufferPool;
    public TableManager(BufferPool bp) {
        this.bufferPool = bp;
    }


    public String getTableName(int fid) {
        return "test";
    }

    public Table getTable(String name) {
        return new Table(name, new Schema(), bufferPool);
    }
//    public Table getTestTable() {
//        Schema schema = new Schema()
//                .add(new Column(DataType.STRING, "name"))
//                .add(new Column(DataType.INTEGER, "age"));
//        return new Table("test.db", schema, bufferPool);
//    }

//    public static Table testTable;
//    static {
//        BufferPool bufferPool = BufferPool.getInstance();
//        var disk = bufferPool.getDisk();
//        disk.newFile(777,"test.db");
//        disk.newFile(369,"log");
//        Schema schema = new Schema();
//        schema.add(new Column(DataType.STRING, "name"));
//        schema.add(new Column(DataType.INTEGER, "age"));
//        RecoveryManager.getInstance().setLogManager(new LogManager(bufferPool));
//        testTable= new Table("test.db", schema, bufferPool);
//    }

}
