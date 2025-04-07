package com.jdb.index;

import com.jdb.catalog.Schema;
import com.jdb.common.Value;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Page;
import com.jdb.table.Record;
import com.jdb.table.*;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static com.jdb.common.Constants.NULL_PAGE_ID;
import static com.jdb.common.Constants.SLOT_SIZE;

public abstract class Node {
    protected IndexMetaData metaData;
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

    public static Node load(IndexMetaData metaData,int pageId) {
        BufferPool bp = BufferPool.getInstance();
        Page page = bp.getPage(metaData.getTableName(), pageId);
        byte[] data = page.getData();
        return switch (data[0]) {
            case 0x01 -> new InnerNode(metaData,pageId, page);
            case 0x02 -> new LeafNode(metaData,pageId, page);
            default -> throw new UnsupportedOperationException("unknown page type");
        };
    }

    public abstract IndexEntry search(Value<?> key);

    public abstract int insert(IndexEntry entry);

    public abstract Value<?> getFloorKey();

    public abstract void readFromPage(int pageId);

    public abstract void write2Page();
}

class InnerNode extends Node {

    IndexPage indexPage;
    public InnerNode(IndexMetaData metaData,int pageId, Page page) {
        this.page = page;
        this.pageId = pageId;
        this.metaData = metaData;
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

        Node child = Node.load(metaData,pid);
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

        Node child = Node.load(metaData,pid);

        int newChildPid = child.insert(entry);
        if (newChildPid == NULL_PAGE_ID) {
            return NULL_PAGE_ID;
        }
        Node newChild = Node.load(metaData,newChildPid);
        indexPage.insert(new SecondaryIndexEntry(newChild.getFloorKey(),new RecordID(newChildPid,0)));

//        if (indexPage.getEntryCount() >= 100) {
//            IndexPage niPage = indexPage.split();
//            niPage.insert(new SecondaryIndexEntry(entry.getKey(), p));
//            return niPage.getPageId();
//        }


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
    private DataPage dataPage;

    LeafNode(IndexMetaData metaData,int pageId, Page page) {
        super();
        this.metaData = metaData;
        this.page = page;
        this.pageId = pageId;

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
                r = dataPage.getRecord(mid,metaData.getTableSchema());
                return new ClusterIndexEntry(Value.ofInt(mk), r);
            }
        }
        throw new NoSuchElementException("key not found");
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

        LeafNode newLeaf = new LeafNode(metaData,newPage.pid, newPage);

        int splitIndex = entries.size() / 2;

        newLeaf.entries = new ArrayList<>(entries.subList(splitIndex, entries.size()));
        entries = new ArrayList<>(entries.subList(0, splitIndex));

        newLeaf.nextLeaf = this.nextLeaf;
        this.nextLeaf = newLeaf;

        return newLeaf;
    }
}
