package com.jdb.index;

import com.jdb.catalog.ColumnList;
import com.jdb.common.Value;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Page;
import com.jdb.table.Record;
import com.jdb.table.*;

import java.util.ArrayList;
import java.util.List;

import static com.jdb.common.Constants.NULL_PAGE_ID;
import static com.jdb.common.Constants.SLOT_SIZE;
/*
 * TODO 内部节点children保存子节点的页号
 * TODO 用代理模式，写两种page的代理类，内部页和数据页，再写read和write方法
 *
 *
 *
 *
 *
 *
 *
 * */

public abstract class Node {
    static final int ORDER = 4;
    public int pageId;
    Page page;
    //不应该保存对页对象的引用，而是从BufferPool随用随取，否则会造成大量内存无法回收，
//    Page page;
//    List<Value<?>> keys;
    String tableName = "test";

    Node() {
//        keys = new ArrayList<>();
    }

    public static Node load(int pageId) {
        BufferPool bp = BufferPool.getInstance();
        Page page = bp.getPage(pageId);
        byte[] data = page.getData();
        switch (data[0]) {
            case 0x01:
                return new InnerNode(pageId, page);
            case 0x02:
                return new LeafNode(pageId, page);
            default:
                throw new RuntimeException("unknown page type");
        }
    }

    public abstract IndexEntry search(Value<?> key);

    public abstract int insert(IndexEntry entry);

    public abstract Value<?> getFloorKey();

    public abstract void readFromPage(int pageId);

    public abstract void write2Page();
}

class InnerNode extends Node {

    IndexPage indexPage;

    public InnerNode(int pageId, Page page) {
        this.page = page;
        this.pageId = pageId;
        indexPage = new IndexPage(pageId, page);
    }

    @Override
    public IndexEntry search(Value<?> key) {
//        int i = Collections.binarySearch(keys, key);
//        if (i >= 0)
//            return children.get(i + 1).search(key);
//        else return children.get(-i - 1).search(key);
        int eid = indexPage.binarySearch(key);
//        if(eid<0)
////            throw new RuntimeException("key not found");
//            eid = -eid;
        IndexEntry floorEntry = indexPage.getEntry(eid);
        RecordID rid = (RecordID) floorEntry.getValue();
        int pid = rid.pageId;

        Node child = Node.load(pid);
        return child.search(key);
    }

    @Override
    public int insert(IndexEntry entry) {
//        int childIndex = findChild(entry.getKey());
//        int childId = children.get(childIndex);
        int eid = indexPage.binarySearch(entry.getKey());
//        Node newChile = child.insert(entry);
//        if (newChile == null) {
//            return null;
//        }
//        if(eid>=0)
//            throw new RuntimeException("key already exist");
//
//        eid = -eid-1;
        IndexEntry floorEntry = indexPage.getEntry(eid);
        RecordID p = (RecordID) floorEntry.getValue();
        int pid = p.pageId;

        Node child = Node.load(pid);

        int newChild = child.insert(entry);
        if (newChild == NULL_PAGE_ID) {
            return NULL_PAGE_ID;
        }
        //子节点分裂后，将新节点最小键和指针加入当前节点
//        keys.add(childIndex, newChile.getMinKey());
//        children.add(childIndex + 1, newChild);
//        if (keys.size() >= ORDER) {
//            //分裂内部节点
//            return split();
//        }
        if (indexPage.getEntryCount() >= 100) {
            IndexPage niPage = indexPage.split();
            niPage.insert(new SecondaryIndexEntry(entry.getKey(), p));
            return niPage.getPageId();
        }

        indexPage.insert(new SecondaryIndexEntry(entry.getKey(), p));

        //无需分裂
        return NULL_PAGE_ID;
    }

    @Override
    public Value<?> getFloorKey() {
        return indexPage.getFloorKey();
    }

    @Override
    public void readFromPage(int pageId) {
        //TODO

    }

    @Override
    public void write2Page() {

    }

//    private InnerNode split() {
//        InnerNode newNode = new InnerNode();
//        int splitIndex = keys.size() / 2;
//
//        newNode.keys = new ArrayList<>(keys.subList(splitIndex + 1, keys.size()));
//        newNode.children = new ArrayList<>(children.subList(splitIndex + 1, children.size()));
//
//        keys = new ArrayList<>(keys.subList(0, splitIndex));
//        children = new ArrayList<>(children.subList(0, splitIndex + 1));
//
//        return newNode;
//    }

//    private int findChild(Value<?> key) {
//        int i = Collections.binarySearch(keys, key);
//        if (i >= 0) {
//            return i;
////            throw new RuntimeException("key already exist");
//        }
//        return -i - 1;
//    }
}

class LeafNode extends Node {
    private List<IndexEntry> entries;
    private LeafNode nextLeaf;
    private int pageId;
    private DataPage dataPage;

    LeafNode(int pageId, Page page) {
        super();
        this.page = page;
        this.pageId = pageId;
//        entries = new ArrayList<>();

        //for test 从页中撸出来，叶子节点暂时只能放数据页
//        DataPage dataPage = new DataPage(page);
//        int cnt = dataPage.getRecordCount();
//        for (int i = 0; i < cnt; i++) {
//            Record record = dataPage.getRecord(i, ColumnList.instance);
//            entries.add(new ClusterIndexEntry(Value.ofInt(record.getPrimaryKey()), record));
//        }

        dataPage = new DataPage(page);
    }

    @Override
    public IndexEntry search(Value<?> key) {
        int low = 0, high = dataPage.getRecordCount() - 1;
        Record r;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            Slot slot = dataPage.getSlot(mid);
            int mk = slot.getPrimaryKey();
            if (key.getValue(Integer.class) < mk)
                high = mid - 1;
            else if (key.getValue(Integer.class) > mk)
                low = mid + 1;
            else {
                r = dataPage.getRecord(mid, ColumnList.instance);
                return new ClusterIndexEntry(Value.ofInt(mk), r);
            }
        }
        throw new RuntimeException("key not found");
    }

    @Override
    public int insert(IndexEntry entry) {
//        int insertIndex = Collections.binarySearch(entries, entry);
//        //暂时只能插入唯一键
//        if (insertIndex >= 0) {
//            throw new RuntimeException("key already exist");
//        }
//        insertIndex = -insertIndex - 1;
//        entries.add(insertIndex, entry);

        if (entry instanceof ClusterIndexEntry) {
            DataPage dataPage = new DataPage(page);
            Record record = ((ClusterIndexEntry) entry).getRecord();
            if (dataPage.getFreeSpace() < record.getSize() + SLOT_SIZE) {
                //空间不足，页分裂
                DataPage newDataPage = dataPage.split();
                newDataPage.insertRecord(record);
                return newDataPage.getPageId();
            } else {
                //插入页中
                dataPage.insertRecord(record);
                return NULL_PAGE_ID;
            }
        } else {

        }


        return NULL_PAGE_ID;
    }

    @Override
    public Value<?> getFloorKey() {
        int key = dataPage.getSlot(0).getPrimaryKey();
        return Value.ofInt(key);
    }

    @Override
    public void readFromPage(int pageId) {
//        Page page = BufferPool.getInstance().getPage(pageId);
//        DataPage dataPage = new DataPage(page);
//        int n = dataPage.getRecordCount();
//        for (int i = 0; i < n; i++) {
//            Slot slot = dataPage.getSlot(i);
//            entries.add(new IndexEntry(Value.ofInt(slot.getPrimaryKey()), new PagePointer(pageId, i)));
//        }
    }

    @Override
    public void write2Page() {

    }

    private LeafNode split() {
        BufferPool bp = BufferPool.getInstance();
        Page newPage = bp.newPage(tableName);

        LeafNode newLeaf = new LeafNode(newPage.pid, newPage);

        int splitIndex = entries.size() / 2;

        newLeaf.entries = new ArrayList<>(entries.subList(splitIndex, entries.size()));
        entries = new ArrayList<>(entries.subList(0, splitIndex));

        newLeaf.nextLeaf = this.nextLeaf;
        this.nextLeaf = newLeaf;

        return newLeaf;
    }
}
