package com.jdb.index;

import com.jdb.common.Value;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Page;
import com.jdb.table.DataPage;
import com.jdb.table.IndexPage;
import com.jdb.table.Table;

import java.util.List;

import static com.jdb.common.Constants.NULL_PAGE_ID;

public class BPTree implements Index {
    private Table table;
    private BufferPool bufferPool;
    private Node root;
    private IndexMetaData metaData;

    public BPTree(IndexMetaData metaData) {
        bufferPool = BufferPool.getInstance();
        Page page = bufferPool.newPage(metaData.getTableName());
        DataPage dataPage = new DataPage(page);
        dataPage.init();
        this.metaData = metaData;
        root = new LeafNode(metaData, dataPage.getPageId(), page);
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
    public void insert(IndexEntry entry) {
        long newNode = root.insert(entry);
        if (newNode != NULL_PAGE_ID) {
            //分裂根节点

            //新建一个索引页
            Page newpage = bufferPool.newPage(metaData.getTableName());
            IndexPage nipage = new IndexPage(newpage.pid, newpage);
            nipage.init();

            //新建内部节点，作为新的root
            InnerNode newRoot = new InnerNode(metaData, newpage.pid, newpage);

            //撸出新节点的两个儿子
            Node c2 = Node.load(metaData, newNode);

            //写入到新节点中 (*´∀`)~♥
            nipage.addChild(0, root.pid);
            nipage.insert(c2.getFloorKey(), newNode);


            //更新root
            root = newRoot;
        }
    }

    @Override
    public void delete(Value<?> key) {

    }


}
