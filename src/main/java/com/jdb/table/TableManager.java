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

import static com.jdb.common.Constants.*;

public class TableManager {
    //for test
//    private static TableManager instance;
//
//    public static TableManager getInstance() {
//        if (instance == null) {
//            instance = new TableManager(BufferPool.getInstance(), Disk.getInstance(), RecoveryManager.getInstance());
//        }
//        return instance;
//    }

    Table tableMeta;
    Table indexMeta;
    Table fileMeta;

    public Table getTableMeta() {
        return tableMeta;
    }

    public Table getIndexMeta() {
        return indexMeta;
    }

    public Table getFileMeta() {
        return fileMeta;
    }


    Map<String, Table> tables = new HashMap<>();

    BufferPool bufferPool;
    RecoveryManager recoveryManager;
    Disk disk;

    public TableManager(BufferPool bp, Disk disk, RecoveryManager rm, boolean initialized) {
        this.bufferPool = bp;
        this.disk = disk;
        this.recoveryManager = rm;

        if (!initialized) {
            init();
        } else {
            load();
        }
    }

    public void init() {
        tableMeta = Table.createInMemory(TABLE_META_DATA_FILE_NAME, TABLE_META_DATA_FILE_ID, TableMataSchema(), bufferPool, recoveryManager);
        indexMeta = Table.createInMemory(INDEX_META_DATA_FILE_NAME, INDEX_META_DATA_FILE_ID, IndexMataSchema(),bufferPool, recoveryManager);
        fileMeta = Table.createInMemory(FILE_META_DATA_FILE_NAME, FILE_META_DATA_FILE_ID, fileMataSchema(),bufferPool, recoveryManager);
    }

    public void load() {
        tableMeta = Table.loadFromDisk(TABLE_META_DATA_FILE_NAME, TABLE_META_DATA_FILE_ID, TableMataSchema(),bufferPool, recoveryManager);
        indexMeta = Table.loadFromDisk(INDEX_META_DATA_FILE_NAME, INDEX_META_DATA_FILE_ID, IndexMataSchema(),bufferPool, recoveryManager);
        fileMeta = Table.loadFromDisk(FILE_META_DATA_FILE_NAME, FILE_META_DATA_FILE_ID, fileMataSchema(),bufferPool, recoveryManager);
    }

    private Schema TableMataSchema() {
        return new Schema()
                .add(new Column(DataType.STRING, "table_name"))
                .add(new Column(DataType.INTEGER, "file_id"))
                .add(new Column(DataType.STRING, "schema"));
    }

    private Schema IndexMataSchema() {
        return new Schema()
                .add(new Column(DataType.STRING, "index_name"))
                .add(new Column(DataType.INTEGER, "file_id"))
                .add(new Column(DataType.STRING, "table_name"))
                .add(new Column(DataType.STRING, "column_name"));
    }

    private Schema fileMataSchema() {
        return new Schema()
                .add(new Column(DataType.INTEGER, "file_id"))
                .add(new Column(DataType.STRING, "file_name"));
    }



    public String getTableName(int fid) {
        var iter = tableMeta.scan();
        while (iter.hasNext()) {
            var rowData = iter.next();
            if (rowData.values.get(1).equals(Value.of(fid))) {
                return rowData.values.get(0).toString();
            }
        }
        throw new NoSuchElementException("table no exist");
    }

    public Table getTable(String name) {
        return tables.get(name);
    }

    public Table getTable(int fid) {
        String name = getTableName(fid);
        return tables.get(name);
    }

    public void create(String tableName, Schema schema) {
        int fid = disk.addFile(tableName + TABLE_FILE_SUFFIX);
        Table table = Table.createInMemory(tableName, fid, schema, bufferPool, recoveryManager);
        tables.put(tableName, table);

        //meta写记录
        var row = new RowData(Value.of(tableName), Value.of(fid), Value.of(schema.toString()));
        tableMeta.insertRecord(row, true, true);
    }

    public void drop(String tableName) {
    }

    public void createIndex(String tableName, String columnName) {
    }

    public void dropIndex(String tableName, String columnName) {
    }

}
