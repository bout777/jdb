package com.jdb.table;

import com.jdb.catalog.Column;
import com.jdb.catalog.Schema;
import com.jdb.common.DataType;
import com.jdb.common.Value;
import com.jdb.index.*;
import com.jdb.recovery.RecoveryManager;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Page;
import com.jdb.version.ReadResult;
import com.jdb.version.VersionManager;

import java.util.Iterator;
import java.util.Map;

public class Table {
//    private static Table instance;
//
//    public static Table getTestTable() {
//        if (instance == null) {
//            Schema schema = new Schema()
//                    .add(new Column(DataType.STRING, "name"))
//                    .add(new Column(DataType.INTEGER, "age"));
//
//            instance = new Table("test.db", 777, schema, BufferPool.getInstance(), RecoveryManager.getInstance());
//        }
//        return instance;
//    }

    private record TableMeta(int fid, String name, Schema schema) {
    }

    TableMeta meta;
    BufferPool bufferPool;
    RecoveryManager recoveryManager;
    VersionManager versionManager;
    BPTree clusterIndex;
    Map<String, Index> secIndices;

    public Schema getSchema() {
        return meta.schema;
    }

    public Table(String name, int fid, Schema schema, BufferPool bp, RecoveryManager rm,VersionManager vm) {
        this.meta = new TableMeta(fid, name, schema);
        this.bufferPool = bp;
        this.recoveryManager = rm;
        this.versionManager = vm;
        IndexMetaData clusterIdxMeta = new IndexMetaData(getTableName(), schema.columns().get(0), getTableName(), schema, fid);
        clusterIndex = new BPTree(clusterIdxMeta, bp, rm);
    }
    public static Table createInMemory(String name, int fid, Schema schema, BufferPool bp, RecoveryManager rm,VersionManager vm){
        var table = new Table(name, fid, schema, bp, rm,vm);
        table.getClusterIndex().init();
        return table;
    }

    public static Table loadFromDisk(String name, int fid, Schema schema, BufferPool bp, RecoveryManager rm,VersionManager vm){

        var table = new Table(name, fid, schema, bp, rm,vm);
        table.getClusterIndex().load();
        return table;
    }


    public BPTree getClusterIndex() {
        return clusterIndex;
    }


    public String getTableName() {
        return meta.name;
    }

    // TODO 把get相关的逻辑弄好，测试，下一步才可以对接索引
    public RowData getRowData(PagePointer ptr) {
        Page page = bufferPool.getPage(ptr.pid);
        return RowData.deserialize(page.getBuffer(), ptr.sid, meta.schema);
    }

    public RowData getRowData(Value<?> key) {
        var vm = versionManager;
        var result = vm.read(meta.name, key);
        if (result.getVisibility() == ReadResult.Visibility.VISIBLE) {
            return result.getRowData();
        } else if (result.getVisibility() == ReadResult.Visibility.INVISIBLE) {
            return null;
        }
        IndexEntry entry = clusterIndex.searchEqual(key);
        return ((ClusterIndexEntry) entry).getRecord();
    }

    //需要改成走索引插入

    public void insertRecord(RowData rowData, boolean shouldLog, boolean shouldPushVersion) {
        if (shouldPushVersion) {
            var vm = versionManager;
            vm.pushUpdate(meta.name, rowData);
        }

        var entry = new ClusterIndexEntry(rowData.getPrimaryKey(), rowData);
        clusterIndex.insert(entry, shouldLog);
    }

    public void updateRecord(Value<?> key, RowData rowData, boolean shouldLog) {
        var vm = versionManager;
        vm.pushUpdate(meta.name, rowData);

        //todo 暂时先删除再插入以实现更新，这样做代码复杂度比较小，后续在页内添加空闲槽位管理，能最大化利用空间减少复杂度
        clusterIndex.delete(key, shouldLog);
        clusterIndex.insert(new ClusterIndexEntry(key, rowData), shouldLog);
    }

    public void deleteRecord(Value<?> key, boolean shouldLog) {
        clusterIndex.delete(key, shouldLog);
    }

    public Iterator<RowData> scan() {
        return clusterIndex.scanAll();
    }
}
