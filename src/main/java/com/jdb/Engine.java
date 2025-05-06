package com.jdb;

import com.jdb.catalog.Schema;
import com.jdb.common.value.Value;
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
import java.util.Iterator;
import java.util.NoSuchElementException;

public class Engine {

    private Table tableMetadata;

    private Table indexMetadata;


    //    public static Engine instance;
    private Disk disk;

    private BufferPool bufferPool;

    private LogManager logManager;

    private RecoveryManager recoveryManager;

    private TableManager tableManager;

    private TransactionManager transactionManager;

    private VersionManager versionManager;

    private String dbDir;

    public Engine(String path) {
        this.dbDir = path;
        boolean initialized = setupDirectory(path);

//        disk = new Disk(path);
//        bufferPool = new BufferPool(disk);
//
//        recoveryManager = new RecoveryManager(bufferPool);
//        disk.setRecoveryManager(recoveryManager);
//
//        recoveryManager.setEngine(this);
//        recoveryManager.setLogManager(new LogManager(bufferPool));
//        if (!initialized) recoveryManager.init();
//
//
//
//        tableManager = new TableManager(bufferPool,disk,recoveryManager,initialized);
//
//        disk.setFileTable(tableManager.getFileMeta());
//        if(initialized)disk.load();
//
//        versionManager = new VersionManager();
//        transactionManager = new TransactionManager(recoveryManager,versionManager);
//
//        recoveryManager.restart();

        managerInstantiation();
        dependencyInjection();

        //读or创建一些必须的文件
        /*
        * 如果文件不存在
        * 则向磁盘写入
        * 如果存在则读取到file对象中
        * 日志文件，索引表文件，文件表文件，表信息表文件
        * 不会产生日志*/
        disk.init();


        /*
        * 向缓冲池申请第一张日志页
        * 写入第一页主日志*/
        if(!initialized)
            logManager.init();

        /*
        * 启动初始化事务
        * 由于tableMan的init方法是新建三张表
        * 会产生日志，所以要关联事务
        * 如果已经初始化，会执行恢复，也要关联事务
        * 所以就在这里先启动一个
        * todo 已分配事务id持久化
        * */
        transactionManager.begin();
        if (!initialized) {
            //创建系统表
            /*
            * 新建三张表
            * 文件表，索引表，表信息表*/
            tableManager.init();

            /*
            * 所有页刷盘
            * */
            bufferPool.flush();
        } else {
            //从磁盘中读文件表，索引表，表信息表
            tableManager.load();
        }
        //执行崩溃恢复
        if(initialized) {
            recoveryManager.restart();
        }
        //提交
        transactionManager.commit();
        //注入文件表
        disk.setFileTable(tableManager.getFileMeta());
        //从文件表中读文件列表
        disk.load();
    }


    public void managerInstantiation() {
        disk = new Disk(this);
        bufferPool = new BufferPool(this);
        recoveryManager = new RecoveryManager(this);
        tableManager = new TableManager(this);
        transactionManager = new TransactionManager(this);
        versionManager = new VersionManager(this);
        logManager = new LogManager(this);
    }

    public void dependencyInjection() {
        disk.injectDependency();
        bufferPool.injectDependency();
        recoveryManager.injectDependency();
        tableManager.injectDependency();
        transactionManager.injectDependency();
        versionManager.injectDependency();
        logManager.injectDependency();
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

    public LogManager getLogManager() {
        return logManager;
    }

    public Disk getDisk() {
        return disk;
    }

    public String getDir() {
        return dbDir;
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


    //======api======//

    public void beginTransaction() {
        transactionManager.begin();
    }

    public void commit() {
        transactionManager.commit();
    }

    public void abort() {
        transactionManager.abort();
    }

    public Iterator<RowData> scan(String tableName, String columnName) {
        Table table = tableManager.getTable(tableName);
        //todo  暂时只智齿主键扫描
        var iter = table.scan();
        return iter;
    }

    public Iterator<RowData> lookup(String tableName, String columnName, Value key) {
        Table table = tableManager.getTable(tableName);
        return null;
    }

//    public Iterator<RowData> scanFrom(String tableName, String columnName, Value key) {
//
//    }


    public void insert(String tableName, RowData rowData) {
        Table table = null;
        try {
            table = tableManager.getTable(tableName);
        } catch (NoSuchElementException e) {
            throw new DatabaseException(e.getMessage());
        }
        table.insertRecord(rowData, true, true);
    }

    public void update(String tableName, Value<?> key, RowData rowData) {
        var table = tableManager.getTable(tableName);
        table.updateRecord(key, rowData, true);
    }

    public void delete(String tableName, Value<?> key) {
        var table = tableManager.getTable(tableName);
        table.deleteRecord(key, true);
    }

    public Table createTable(String tableName, Schema schema) {
         return tableManager.create(tableName, schema);
    }

    public void dropTable(String tableName) {
        tableManager.drop(tableName);
    }

    public void createIndex(String tableName, String columnName) {
    }

    public void dropIndex(String tableName, String columnName) {
    }

    public void close() {
        bufferPool.close();
        disk.close();
    }
}
