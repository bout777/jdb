package com.jdb.index;

import com.jdb.common.Value;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Page;
import com.jdb.table.DataPage;
import com.jdb.table.IndexPage;
import com.jdb.table.RecordID;

import static com.jdb.common.Constants.NULL_PAGE_ID;

public class BPTree implements Index {
    private BufferPool bufferPool;
    private Node root;
    private String tableName = "test";
    public BPTree(BufferPool bp) {
        bufferPool = bp;
        Page page = bufferPool.newPage(tableName);
        DataPage dataPage = new DataPage(page);
        dataPage.init();
        root = new LeafNode(dataPage.getPageId(), page);
    }

    @Override
    public IndexEntry searchEqual(Value<?> key) {
        return root.search(key);
    }

    @Override
    public IndexEntry searchRange(Value<?> low, Value<?> high) {
        return null;
    }

    @Override
    public void insert(IndexEntry entry) {
        int newNode = root.insert(entry);
        if (newNode != NULL_PAGE_ID) {
            //分裂根节点

            //新建一个索引页

            Page newpage = bufferPool.newPage(tableName);

            IndexPage nipage = new IndexPage(newpage.pid, newpage);
            nipage.init();

            //新建内部节点，作为新的root
            InnerNode newRoot = new InnerNode(newpage.pid, newpage);

            //撸出新节点的两个儿子
            RecordID p1 = new RecordID(root.pageId, 0);
            RecordID p2 = new RecordID(newNode, 0);
            Node c2 = Node.load(newNode);
            IndexEntry e1 = new SecondaryIndexEntry(root.getFloorKey(), p1);
            IndexEntry e2 = new SecondaryIndexEntry(c2.getFloorKey(), p2);

            //插入到新节点中 (*´∀`)~♥
            nipage.insert(e1);
            nipage.insert(e2);

            //更新root
            root = newRoot;
        }
    }

    @Override
    public void delete(IndexEntry entry) {

    }
}
