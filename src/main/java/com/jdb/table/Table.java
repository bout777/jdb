package com.jdb.table;

import com.jdb.catalog.Schema;
import com.jdb.common.Value;
import com.jdb.index.*;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Page;
import com.jdb.version.ReadResult;
import com.jdb.version.VersionManager;

import static com.jdb.common.Constants.NULL_PAGE_ID;

public class Table {
    BufferPool bufferPool;
    long firstPageId = NULL_PAGE_ID;
    Index clusterIndex;

    public Schema getSchema() {
        return schema;
    }

    Schema schema;
    String tableName = "test";
    public Table(String name, Schema schema) {
        this.bufferPool = BufferPool.getInstance();
        this.schema = schema;
        this.tableName = name;

        IndexMetaData metaData = new IndexMetaData(getTableName(),schema.columns().get(0),"test",schema);
        clusterIndex = new BPTree(metaData);
    }


    public Index getClusterIndex() {
        return clusterIndex;
    }


    public String getTableName() {
        return tableName;
    }

    // TODO 把get相关的逻辑弄好，测试，下一步才可以对接索引
    public RowData getRowData(PagePointer ptr) {
        Page page = bufferPool.getPage(ptr.pid);
        return RowData.deserialize(page.getBuffer(), ptr.sid, schema);
    }

    public RowData getRowData(Value<?> key) {
        var vm = VersionManager.getInstance();
        var result = vm.read(tableName, key);
        if(result.getVisibility()==ReadResult.Visibility.VISIBLE){
            return result.getRowData();
        }else if(result.getVisibility()==ReadResult.Visibility.INVISIBLE){
            return null;
        }
        IndexEntry entry = clusterIndex.searchEqual(key);
        return ((ClusterIndexEntry) entry).getRecord();
    }

    //需要改成走索引插入
//    public void insertRecord(RowData rowData) {
//        //表还没有页面，创建一个
//        if (firstPageId == NULL_PAGE_ID) {
//            firstPageId = 0;
//            bufferPool.newPage(tableName);
//            DataPage dataPage = new DataPage(bufferPool.getPage(firstPageId));
//            dataPage.init();
//        }
//
//        long pid = firstPageId;
//
//        DataPage dataPage = new DataPage(bufferPool.getPage(pid));
//
//        //遍历页面，找到一个能插入的
//        while (dataPage.getFreeSpace() < rowData.getSize() + SLOT_SIZE && dataPage.getNextPageId() != NULL_PAGE_ID) {
//            pid++;
//            dataPage = new DataPage(bufferPool.getPage(pid));
//        }
//        //如果找不到，创建一个新页面
//        if (dataPage.getFreeSpace() < rowData.getSize() + SLOT_SIZE && dataPage.getNextPageId() == NULL_PAGE_ID) {
//            pid++;
//            dataPage.setNextPageId(pid);
//            Page page = bufferPool.newPage(tableName);
//            dataPage = new DataPage(page);
//            dataPage.init();
//        }
//
//        //插入record
//        dataPage.insertRecord(rowData, true, true);
//    }

    public void insertRecord(RowData rowData, boolean shouldLog, boolean shouldPushVersion){
        var vm = VersionManager.getInstance();
        vm.pushUpdate(tableName, rowData);

        var entry = new ClusterIndexEntry(Value.ofInt(rowData.getPrimaryKey()), rowData);
        clusterIndex.insert(entry, shouldLog);
    }

    public void updateRecord(Value<?> key, RowData rowData,boolean shouldLog){
        var vm = VersionManager.getInstance();
        vm.pushUpdate(tableName, rowData);

        //todo 暂时先删除再插入以实现更新，这样做代码复杂度比较小，后续在页内添加空闲槽位管理，能最大化利用空间减少复杂度
        clusterIndex.delete(key, shouldLog);
        clusterIndex.insert(new ClusterIndexEntry(key, rowData),shouldLog);
    }
    public void deleteRecord(Value<?> key,boolean shouldLog) {
        clusterIndex.delete(key,shouldLog);
    }
}
