package com.jdb.table;

import com.jdb.catalog.Column;
import com.jdb.catalog.Schema;
import com.jdb.common.DataType;
import com.jdb.recovery.LogManager;
import com.jdb.recovery.RecoveryManager;
import com.jdb.storage.BufferPool;

public class TableManager {
    private static TableManager instance = new TableManager();
    public static TableManager getInstance() {
        return instance;
    }


    public String getTableName(int fid) {
        return "test";
    }

    public Table getTable(String name) {
        return new Table(name, new Schema());
    }
    public Table getTable(int fid) {
        return testTable;
    }

    public static Table testTable;
    static {
        BufferPool bufferPool = BufferPool.getInstance();
        bufferPool.newFile(777,"test.db");
        bufferPool.newFile(369,"log");
        Schema schema = new Schema();
        schema.add(new Column(DataType.STRING, "name"));
        schema.add(new Column(DataType.INTEGER, "age"));
        RecoveryManager.getInstance().setLogManager(new LogManager());
        testTable= new Table("test.db", schema);
    }

}
