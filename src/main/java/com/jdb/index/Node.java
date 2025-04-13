package com.jdb.index;

import com.jdb.common.PageHelper;
import com.jdb.common.Value;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Page;
import com.jdb.table.DataPage;
import com.jdb.table.IndexPage;
import com.jdb.table.Record;
import com.jdb.table.Slot;

import java.util.List;
import java.util.NoSuchElementException;

import static com.jdb.common.Constants.NULL_PAGE_ID;
import static com.jdb.common.Constants.SLOT_SIZE;

public abstract class Node {
    protected IndexMetaData metaData;
    static final int ORDER = 4;
    public long pid;
    public int fid;
    Page page;
    //不应该保存对页对象的引用，而是从BufferPool随用随取，否则会造成大量内存无法回收，
//    Page page;
//    List<Value<?>> keys;
    String tableName = "test";

    Node() {
//        keys = new ArrayList<>();
    }

    public static Node load(IndexMetaData metaData, long pid) {
        BufferPool bp = BufferPool.getInstance();
        Page page = bp.getPage(pid);
        byte[] data = page.getData();
        return switch (data[0]) {
            case 0x01 -> new InnerNode(metaData, pid, page);
            case 0x02 -> new LeafNode(metaData, pid, page);
            default -> throw new UnsupportedOperationException("unknown page type");
        };
    }

    public abstract IndexEntry search(Value<?> key);

    public abstract long insert(IndexEntry entry);

    public abstract Value<?> getFloorKey();

    public abstract void readFromPage(int pageId);

    public abstract void write2Page();
}

class InnerNode extends Node {

    IndexPage indexPage;

    public InnerNode(IndexMetaData metaData, long pid, Page page) {
        this.page = page;
        this.pid = pid;
        this.fid = PageHelper.getFid(pid);
        this.metaData = metaData;
        indexPage = new IndexPage(pid, page);
    }

    @Override
    public IndexEntry search(Value<?> key) {
        int cno = indexPage.binarySearch(key) + 1;
        int pno = indexPage.getChild(cno);
        long pid = PageHelper.concatPid(this.fid, pno);
        Node child = Node.load(metaData, pid);
        return child.search(key);
    }

    @Override
    public long insert(IndexEntry entry) {
        int cno = indexPage.binarySearch(entry.getKey()) + 1;
        int pno = indexPage.getChild(cno);
        long pid = PageHelper.concatPid(this.fid, pno);
        Node child = Node.load(metaData, pid);
        long newChildPid = child.insert(entry);
        if (newChildPid == NULL_PAGE_ID) {
            return NULL_PAGE_ID;
        }
        Node newChild = Node.load(metaData, newChildPid);
        indexPage.insert(newChild.getFloorKey(), newChildPid);
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

    LeafNode(IndexMetaData metaData, long pid, Page page) {
        super();
        this.metaData = metaData;
        this.page = page;
        this.pid = pid;

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
                r = dataPage.getRecord(mid, metaData.getTableSchema());
                return new ClusterIndexEntry(Value.ofInt(mk), r);
            }
        }
        throw new NoSuchElementException("key not found");
    }

    @Override
    public long insert(IndexEntry entry) {
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
                if (record.getPrimaryKey() < newDataPage.getSlot(0).getPrimaryKey()) {
                    dataPage.insertRecord(record, true, true);
                } else {
                    newDataPage.insertRecord(record, true, true);
                }
                return newDataPage.getPageId();
            } else {
                //插入页中
                dataPage.insertRecord(record, true, true);
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

}
