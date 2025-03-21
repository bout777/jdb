package com.idme.index;

import com.idme.common.Value;
import com.idme.storage.BufferPool;
import com.idme.storage.Page;
import com.idme.table.DataPage;
import com.idme.table.IndexPage;
import com.idme.table.PagePointer;

import static com.idme.common.Constants.NULL_PAGE_ID;

public class BPTree implements Index {
    private BufferPool bufferPool;
    private Node root;

    public BPTree(BufferPool bp) {
        bufferPool = bp;
        Page page = bufferPool.newPage(bufferPool.getMaxPageId());
        DataPage dataPage = new DataPage(bufferPool.getMaxPageId() - 1, page);
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
//            InnerNode newRoot = new InnerNode();
//            newRoot.keys.add(newNode.getFloorKey());
//            newRoot.children.add(root);
//            newRoot.children.add(newNode);
            //新建一个索引页
            int newpageId = bufferPool.getMaxPageId();
            Page newpage = bufferPool.newPage(newpageId);

            IndexPage nipage = new IndexPage(newpageId, newpage);
            nipage.init();

            //新建内部节点，作为新的root
            InnerNode newRoot = new InnerNode(newpageId, newpage);

            //撸出新节点的两个儿子
            PagePointer p1 = new PagePointer(root.pageId, 0);
            PagePointer p2 = new PagePointer(newNode, 0);
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
