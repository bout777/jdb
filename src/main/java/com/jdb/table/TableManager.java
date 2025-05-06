package com.jdb.table;

import com.jdb.Engine;
import com.jdb.catalog.Column;
import com.jdb.catalog.Schema;
import com.jdb.common.DataType;
import com.jdb.common.value.Value;
import com.jdb.exception.DatabaseException;
import com.jdb.recovery.RecoveryManager;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Disk;
import com.jdb.version.VersionManager;

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
    Engine engine;

    Table tableMeta;
    Table indexMeta;
    Table fileMeta;

    public TableManager(Engine engine) {
        this.engine = engine;
    }

    public void injectDependency() {
        disk = engine.getDisk();
        recoveryManager = engine.getRecoveryManager();
        bufferPool = engine.getBufferPool();
        versionManager = engine.getVersionManager();
    }

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
    VersionManager versionManager;

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
        tableMeta = Table.createInMemory(TABLE_META_DATA_FILE_NAME, TABLE_META_DATA_FILE_ID, TableMataSchema(), bufferPool, recoveryManager,versionManager);
        indexMeta = Table.createInMemory(INDEX_META_DATA_FILE_NAME, INDEX_META_DATA_FILE_ID, IndexMataSchema(),bufferPool, recoveryManager,versionManager);
        fileMeta = Table.createInMemory(FILE_META_DATA_FILE_NAME, FILE_META_DATA_FILE_ID, fileMataSchema(),bufferPool, recoveryManager,versionManager);
    }

    public void load() {
        tableMeta = Table.loadFromDisk(TABLE_META_DATA_FILE_NAME, TABLE_META_DATA_FILE_ID, TableMataSchema(),bufferPool, recoveryManager,versionManager);
        indexMeta = Table.loadFromDisk(INDEX_META_DATA_FILE_NAME, INDEX_META_DATA_FILE_ID, IndexMataSchema(),bufferPool, recoveryManager,versionManager);
        fileMeta = Table.loadFromDisk(FILE_META_DATA_FILE_NAME, FILE_META_DATA_FILE_ID, fileMataSchema(),bufferPool, recoveryManager,versionManager);
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
        Table table = tables.get(name);
        if(table==null){
            var iter = tableMeta.scan();
            while (iter.hasNext()) {
                var rowData = iter.next();
                String tbName = (String) rowData.values.get(0).getValue(String.class);
                if (tbName.equals(name)) {
                    int fid = (int) rowData.values.get(1).getValue(Integer.class);
                    Schema schema = Schema.fromString((String) rowData.values.get(2).getValue(String.class));
                    table= Table.loadFromDisk(name, fid, schema, bufferPool, recoveryManager,versionManager);
                }
            }
        }
        if(table==null)
            throw new NoSuchElementException("table no exist");

        tables.put(name,table);
        return table;
    }

    public Table getTable(int fid) {
        return switch (fid) {
            case TABLE_META_DATA_FILE_ID -> tableMeta;
            case INDEX_META_DATA_FILE_ID -> indexMeta;
            case FILE_META_DATA_FILE_ID -> fileMeta;
            default -> getTableFromMeta(fid);
        };
    }

    private Table getTableFromMeta(int fid) {
        var iter = tableMeta.scan();
        while (iter.hasNext()) {
            var rowData = iter.next();
            int tbFid = (int) rowData.values.get(1).getValue(Integer.class);
            if (tbFid==fid) {
                String tbName = (String) rowData.values.get(0).getValue(String.class);
                if(tables.containsKey(tbName)){
                    return tables.get(tbName);
                }
                Schema schema = Schema.fromString((String) rowData.values.get(2).getValue(String.class));
                var table= Table.loadFromDisk(tbName, fid, schema, bufferPool, recoveryManager,versionManager);
                tables.put(tbName, table);
                return table;
            }
        }
        throw new NoSuchElementException("table no exist");
    }
    //从tableMeta中查schema,避免过早创建table

    public Schema getTableSchema(int fid) {
        return switch (fid) {
            case TABLE_META_DATA_FILE_ID -> TableMataSchema();
            case INDEX_META_DATA_FILE_ID -> IndexMataSchema();
            case FILE_META_DATA_FILE_ID -> fileMataSchema();
            default -> getTableSchemaFromMeta(fid);
        };
    }
    private Schema getTableSchemaFromMeta(int fid){
        var iter = tableMeta.scan();
        while (iter.hasNext()) {
            var rowData = iter.next();
            int tbFid = (int) rowData.values.get(1).getValue(Integer.class);
            if (tbFid==fid) {
                Schema schema = Schema.fromString((String) rowData.values.get(2).getValue(String.class));
                return schema;
            }
        }
        throw new NoSuchElementException("table no exist");
    }

    public Table create(String tableName, Schema schema) {
        //todo 先检查表是否已存在
        int fid = disk.addFile(tableName + TABLE_FILE_SUFFIX);
        Table table = Table.createInMemory(tableName, fid, schema, bufferPool, recoveryManager,versionManager);
        tables.put(tableName, table);

        //meta写记录
        var row = new RowData(Value.of(tableName), Value.of(fid), Value.of(schema.toString()));
        tableMeta.insertRecord(row, true, true);

        return table;
    }

    public void drop(String tableName) {
    }

    public void createIndex(String tableName, String columnName) {
    }

    public void dropIndex(String tableName, String columnName) {
    }

}
