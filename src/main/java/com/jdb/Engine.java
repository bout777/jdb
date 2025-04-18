package com.jdb;

import com.jdb.catalog.Schema;
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

public class Engine {


    private Table tableMetadata;

    private Table indexMetadata;


    private final Disk disk;

    private final BufferPool bufferPool;

    private final RecoveryManager recoveryManager;

    private final TableManager tableManager;

    private final TransactionManager transactionManager;

    private final VersionManager versionManager;

    public Engine(String path) {
        boolean init = setupDirectory(path);
        disk = new Disk(path);
        bufferPool = new BufferPool(disk);
        recoveryManager = new RecoveryManager(bufferPool);
        tableManager = new TableManager(bufferPool);
        transactionManager = new TransactionManager(recoveryManager);
        versionManager = new VersionManager();
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

    public void insert(String tableName, RowData rowData){}

    public void update(String tableName, RowData rowData){}

    public void delete(String tableName, RowData rowData){}

    public void createTable(String tableName, Schema schema){}

    public void dropTable(String tableName){}

    public void createIndex(String tableName, String columnName){}

    public void dropIndex(String tableName, String columnName){}
}
