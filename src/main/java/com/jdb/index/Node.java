package com.jdb.index;

import com.jdb.common.PageHelper;
import com.jdb.common.Value;
import com.jdb.recovery.RecoveryManager;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Page;
import com.jdb.storage.PageType;
import com.jdb.table.DataPage;
import com.jdb.table.IndexPage;
import com.jdb.table.RowData;

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
    BufferPool bufferPool;
    RecoveryManager recoveryManager;

    Node(BufferPool bufferPool,RecoveryManager rm) {
        this.bufferPool = bufferPool;
        this.recoveryManager = rm;
    }

    public static Node load(IndexMetaData metaData, long pid, BufferPool bp, RecoveryManager rm) {
        Page page = bp.getPage(pid);
        byte[] data = page.getData();
        return switch (data[0]) {
            case PageType.DATA_PAGE -> new InnerNode(metaData, pid, page,bp,rm);
            case PageType.INDEX_PAGE -> new LeafNode(metaData, pid, page,bp,rm);
            default -> throw new UnsupportedOperationException("unknown page type");
        };
    }

    public abstract IndexEntry search(Value<?> key);

    public abstract long insert(IndexEntry entry, boolean shouldLog);

    public abstract long delete(Value<?> key, boolean shouldLog);

    public abstract Value<?> getFloorKey();

    public abstract void readFromPage(int pageId);

    public abstract void write2Page();

    public abstract DataPage getFirstLeafPage();
}

class InnerNode extends Node {

    IndexPage indexPage;

    public InnerNode(IndexMetaData metaData, long pid, Page page, BufferPool bp, RecoveryManager rm) {
        super(bp,rm);
        this.page = page;
        this.pid = pid;
        this.fid = PageHelper.getFid(pid);
        this.metaData = metaData;

        indexPage = new IndexPage(pid, page,bufferPool);
    }

    @Override
    public IndexEntry search(Value<?> key) {
        int cno = indexPage.binarySearch(key) + 1;
        int pno = indexPage.getChild(cno);
        long pid = PageHelper.concatPid(this.fid, pno);
        Node child = Node.load(metaData, pid, bufferPool, recoveryManager);
        return child.search(key);
    }

    @Override
    public long insert(IndexEntry entry, boolean shouldLog) {
        int cno = indexPage.binarySearch(entry.getKey()) + 1;
        int pno = indexPage.getChild(cno);
        long pid = PageHelper.concatPid(this.fid, pno);
        Node child = Node.load(metaData, pid,bufferPool,recoveryManager);
        long newChildPid = child.insert(entry, shouldLog);
        if (newChildPid == NULL_PAGE_ID) {
            return NULL_PAGE_ID;
        }
        Node newChild = Node.load(metaData, newChildPid,bufferPool,recoveryManager);
        indexPage.insert(newChild.getFloorKey(), newChildPid);
        //无需分裂
        return NULL_PAGE_ID;
    }

    @Override
    public long delete(Value<?> key, boolean shouldLog) {
        int cno = indexPage.binarySearch(key) + 1;
        int pno = indexPage.getChild(cno);
        long pid = PageHelper.concatPid(this.fid, pno);
        Node child = Node.load(metaData, pid,bufferPool,recoveryManager);
        //todo 页合并
        return child.delete(key, shouldLog);
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
    @Override
    public DataPage getFirstLeafPage() {
        int pno = indexPage.getChild(0);
        long pid = PageHelper.concatPid(this.fid, pno);
        Node child = Node.load(metaData, pid, bufferPool,recoveryManager);
        return child.getFirstLeafPage();
    }
}

class LeafNode extends Node {
    private List<IndexEntry> entries;
    private LeafNode nextLeaf;
    private DataPage dataPage;


    LeafNode(IndexMetaData metaData, long pid, Page page, BufferPool bp,RecoveryManager rm) {
        super(bp,rm);
        this.metaData = metaData;
        this.page = page;
        this.pid = pid;
        dataPage = new DataPage(page,bp, rm,metaData.tableSchema);
    }

    @Override
    public IndexEntry search(Value<?> key) {
        int idx = dataPage.binarySearch(key);
        if(idx<0)
            throw new NoSuchElementException("404 not found");
        RowData r = dataPage.getRecord(idx);
        return new ClusterIndexEntry(r.getPrimaryKey(),r);
    }

    @Override
    public long insert(IndexEntry entry, boolean shouldLog) {
        if (entry instanceof ClusterIndexEntry) {
            DataPage dataPage = new DataPage(page,bufferPool,recoveryManager,metaData.tableSchema);
            RowData rowData = ((ClusterIndexEntry) entry).getRecord();
            if (dataPage.getFreeSpace() < rowData.size() + SLOT_SIZE) {
                //空间不足，页分裂
                DataPage newDataPage = dataPage.split();
                if (rowData.getPrimaryKey().compareTo(newDataPage.getRecord(0).getPrimaryKey())<0) {
                    dataPage.insertRecord(rowData, true, true);
                } else {
                    newDataPage.insertRecord(rowData, true, true);
                }
                return newDataPage.getPageId();
            } else {
                //插入页中
                dataPage.insertRecord(rowData, shouldLog, true);
                return NULL_PAGE_ID;
            }
        } else {

        }


        return NULL_PAGE_ID;
    }

    @Override
    public long delete(Value<?> key, boolean shouldLog) {
        //todo 当页内记录数太少时,页合并
        dataPage.deleteRecord(key, shouldLog);
        return NULL_PAGE_ID;
    }

    @Override
    public Value<?> getFloorKey() {
        var key = dataPage.getRecord(0).getPrimaryKey();
        return key;
    }

//    public int binarySearch(Value<?> key) {
//        int low = 0, high = dataPage.getRecordCount() - 1;
//        while (low <= high) {
//            int mid = (low + high) >>> 1;
//            var midKey = dataPage.getRecord(mid, metaData.getTableSchema()).getPrimaryKey();
//            if (key.compareTo(midKey)<0)
//                high = mid - 1;
//            else if (key.compareTo(midKey)>0)
//                low = mid + 1;
//            else {
//                return mid;
//            }
//        }
//        throw new NoSuchElementException("key not found");
//    }

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

    @Override
    public DataPage getFirstLeafPage(){
        return dataPage;
    }

}
