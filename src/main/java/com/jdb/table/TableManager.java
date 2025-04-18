package com.jdb.table;

import com.jdb.catalog.Column;
import com.jdb.catalog.Schema;
import com.jdb.common.DataType;
import com.jdb.common.Value;
import com.jdb.recovery.RecoveryManager;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Disk;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;

import static com.jdb.common.Constants.TABLE_META_DATA_FILE_ID;

public class TableManager {
    //for test
    private static TableManager instance;

    public static TableManager getInstance() {
        if (instance == null) {
            instance = new TableManager(BufferPool.getInstance(),Disk.getInstance(),RecoveryManager.getInstance());
        }
        return instance;
    }

    Table meta;

    Map<String, Table> tables = new HashMap<>();

    BufferPool bufferPool;
    RecoveryManager recoveryManager;
    Disk disk;
    public TableManager(BufferPool bp,Disk disk,RecoveryManager rm) {
        this.bufferPool = bp;
        this.disk = disk;
        this.recoveryManager = rm;
        loadMeta();
    }

    private void loadMeta() {
        var schema = new Schema()
                .add(new Column(DataType.INTEGER, "fid"))
                .add(new Column(DataType.STRING, "tableName"))
                .add(new Column(DataType.STRING, "schema"));
        meta = new Table("_meta.table", TABLE_META_DATA_FILE_ID, schema, bufferPool,recoveryManager);
    }


    public String getTableName(int fid) {
        var iter = meta.scan();
        while (iter.hasNext()) {
            var rowData = iter.next();
            if (rowData.values.get(0).equals(Value.ofInt(fid))) {
                return rowData.values.get(1).toString();
            }
        }
        throw new NoSuchElementException("table no exist");
    }

    public Table getTable(String name) {
        return tables.get(name);
    }

    public Table getTable(int fid){
        String name = getTableName(fid);
        return tables.get(name);
    }

    public void create(String tableName, Schema schema) {
        Random random = new Random();
        //todo 后续改成从disk获取
        int fid = random.nextInt(1000);
        disk.putFile(fid, tableName);
        Table table = new Table(tableName,fid,schema, bufferPool,recoveryManager);
        tables.put(tableName, table);

        //todo meta写记录
    }

    public void drop(String tableName) {
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
