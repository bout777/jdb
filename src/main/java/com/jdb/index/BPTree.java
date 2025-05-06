package com.jdb.index;

import com.jdb.common.PageHelper;
import com.jdb.common.value.Value;
import com.jdb.recovery.RecoveryManager;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Page;
import com.jdb.table.*;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static com.jdb.common.Constants.NULL_PAGE_ID;

public class BPTree implements Index {
    private Table table;
    private BufferPool bufferPool;
    private RecoveryManager recoveryManager;
    private Node root;
    private IndexMetaData metaData;

    public BPTree(IndexMetaData metaData,BufferPool bp,RecoveryManager rm) {
        this.bufferPool = bp;
        this.metaData = metaData;
        this.recoveryManager = rm;
//        Page page = bufferPool.newPage(metaData.getTableName());
//        DataPage dataPage = new DataPage(page,bufferPool);
//        dataPage.init();
//        root = new LeafNode(metaData, dataPage.getPageId(), page,bufferPool);
    }

    public void init() {
        Page page = bufferPool.newPage(metaData.fid, true);
        MasterPage masterPage = new MasterPage(page,recoveryManager);

        page = bufferPool.newPage(metaData.fid, true);
        DataPage dataPage = new DataPage(page, bufferPool, recoveryManager,metaData.tableSchema);
        dataPage.init();
        //fixme 这样搞不能满足原子性,要整合到日志里
        masterPage.setRootPageId(page.pid);

        root = new LeafNode(metaData, dataPage.getPageId(), page, bufferPool,recoveryManager);
    }

    public void load(){
        var masterPage = new MasterPage(bufferPool.getPage(PageHelper.concatPid(metaData.fid,0)),recoveryManager);
        //fixme 根节点的页号可能会变化,不一定是0
        long rootPid = masterPage.getRootPageId();
        root = Node.load(metaData, rootPid, bufferPool,recoveryManager);
    }

    @Override
    public IndexEntry searchEqual(Value<?> key) {
        return root.search(key);
    }

    @Override
    public List<IndexEntry> searchRange(Value<?> low, Value<?> high) {
        return null;
    }

    @Override
    public void insert(IndexEntry entry, boolean shouldLog) {
        long newNode = root.insert(entry,shouldLog);
        if (newNode != NULL_PAGE_ID) {
            //分裂根节点

            //新建一个索引页
            Page newpage = bufferPool.newPage(metaData.fid, true);
            IndexPage nipage = new IndexPage(newpage,bufferPool, recoveryManager);
            nipage.init();

            //新建内部节点，作为新的root
            InnerNode newRoot = new InnerNode(metaData, newpage.pid, newpage,bufferPool,recoveryManager);

            //撸出新节点的两个儿子
            Node c2 = Node.load(metaData, newNode,bufferPool,recoveryManager);

            //写入到新节点中 (*´∀`)~♥
            nipage.addChild(0, root.pid);
            nipage.insert(c2.getFloorKey(), newNode, true);


            //更新root
            root = newRoot;

            var masterPage = new MasterPage(bufferPool.getPage(PageHelper.concatPid(metaData.fid,0)),recoveryManager);
            masterPage.setRootPageId(root.pid);
//            bufferPool.flushPage(masterPage.getPageId());
        }
    }

    @Override
    public void delete(Value<?> key, boolean shouldLog) {
//        System.out.println("on delete: "+key);
        root.delete(key,shouldLog);
    }

    public Iterator<RowData> scanAll() {
        return new RowDataIterator();
    }

    private class RowDataIterator implements Iterator<RowData> {
        private DataPage dataPage;
        private Iterator<RowData> internalIterator;
        RowDataIterator(){
            this.dataPage = root.getFirstLeafPage();
            this.internalIterator = dataPage.scanFrom(0);

        }
        @Override
        public boolean hasNext() {
            return internalIterator!=null&&internalIterator.hasNext();
        }

        @Override
        public RowData next() {
            if(!hasNext())
                throw new NoSuchElementException();
            RowData rowData = internalIterator.next();
            while (!internalIterator.hasNext()){
                long pid = dataPage.getNextPageId();
                if (pid == NULL_PAGE_ID){
                    internalIterator = null;
                    break;
                }else{
                    dataPage = new DataPage(bufferPool.getPage(pid),bufferPool,recoveryManager,metaData.tableSchema);
                    internalIterator = dataPage.scanFrom(0);
                }
            }
            return rowData;
        }
    }
}
