package com.jdb;

import com.jdb.catalog.Column;
import com.jdb.catalog.Schema;
import com.jdb.common.DataType;
import com.jdb.common.Value;
import com.jdb.exception.DatabaseException;
import com.jdb.recovery.LogManager;
import com.jdb.recovery.RecoveryManager;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Disk;
import com.jdb.table.RowData;
import com.jdb.table.Table;
import com.jdb.table.TableManager;
import com.jdb.transaction.TransactionManager;
import com.jdb.version.VersionManager;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.jdb.common.Constants.LOG_FILE_ID;
import static com.jdb.common.Constants.TABLE_META_DATA_FILE_ID;

public class Engine {

    private Table tableMetadata;

    private Table indexMetadata;

    public static Engine instance;

    private final Disk disk;

    private final BufferPool bufferPool;

    private final RecoveryManager recoveryManager;

    private final TableManager tableManager;

    private final TransactionManager transactionManager;

    private final VersionManager versionManager;

    public Engine(String path) {
        boolean init = setupDirectory(path);
        disk = new Disk(path);
        disk.putFile(LOG_FILE_ID, "log");
        disk.putFile(TABLE_META_DATA_FILE_ID, "_meta.table");

        bufferPool = new BufferPool(disk);

        recoveryManager = new RecoveryManager(bufferPool);
        recoveryManager.setEngine(this);
        recoveryManager.setLogManager(new LogManager(bufferPool));
        if (!init) recoveryManager.init();

        tableManager = new TableManager(bufferPool,disk,recoveryManager);
        transactionManager = new TransactionManager(recoveryManager);
        versionManager = new VersionManager();

//        transactionManager.begin();
//        if (!init) {
//            this.initTableInfo();
//        } else {
//            this.loadMetadata();
//        }
//        transactionManager.commit();
        instance = this;
    }

    private void loadMetadata() {

    }

    private void initTableInfo() {
        bufferPool.newPage(TABLE_META_DATA_FILE_ID);
        tableManager.create("_meta.table", getTableMataSchema());
        tableMetadata = tableManager.getTable("_meta.table");
    }

    private Schema getTableMataSchema() {
        return new Schema()
                .add(new Column(DataType.STRING, "table_name"))
                .add(new Column(DataType.INTEGER, "file_id"))
                .add(new Column(DataType.STRING,"schema"));
    }

    public VersionManager getVersionManager() {
        return versionManager;
    }

    public TransactionManager getTransactionManager() {
        return transactionManager;
    }

    public TableManager getTableManager() {
        return tableManager;
    }

    public RecoveryManager getRecoveryManager() {
        return recoveryManager;
    }

    public BufferPool getBufferPool() {
        return bufferPool;
    }

    public Disk getDisk() {
        return disk;
    }

    private boolean setupDirectory(String path) {
        File dir = new File(path);
        boolean initialized = dir.exists();
        if (!initialized) {
            if (!dir.mkdir()) {
                throw new DatabaseException("failed to create directory " + path);
            }
        } else if (!dir.isDirectory()) {
            throw new DatabaseException(path + " is not a directory");
        }
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(dir.toPath())) {
            initialized = initialized && dirStream.iterator().hasNext();
        } catch (IOException e) {
            throw new DatabaseException(e);
        }
        return initialized;
    }

    public void beginTransaction() {
        transactionManager.begin();
    }

    public void insert(String tableName, RowData rowData) {
        var table = tableManager.getTable(tableName);
        table.insertRecord(rowData,true,true);
    }

    public void update(String tableName, RowData rowData) {
    }

    public void delete(String tableName, RowData rowData) {
        var table = tableManager.getTable(tableName);
        table.deleteRecord(rowData.getPrimaryKey(),true);
    }

    public void createTable(String tableName, Schema schema) {
        tableManager.create(tableName, schema);
    }

    public void dropTable(String tableName) {
        tableManager.drop(tableName);
    }

    public void createIndex(String tableName, String columnName) {
    }

    public void dropIndex(String tableName, String columnName) {
    }
}
