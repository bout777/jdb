package com.idme.index;

import com.idme.common.Value;
import com.idme.storage.BufferPool;

public class BPTree implements Index {
    private BufferPool bufferPool;
    private Node root;

    public BPTree() {
        root = new LeafNode();
    }

    @Override
    public IndexEntry searchEqual(Value<?> key) {
        IndexEntry entry = new IndexEntry(key, null);
        entry.pointer = root.search(key);
        return entry;
    }

    @Override
    public IndexEntry searchRange(Value<?> low, Value<?> high) {
        return null;
    }

    @Override
    public void insert(IndexEntry entry) {
        Node newNode = root.insert(entry);
        if (newNode != null) {
            //分裂根节点
            InnerNode newRoot = new InnerNode();
            newRoot.keys.add(newNode.getMinKey());
            newRoot.children.add(root);
            newRoot.children.add(newNode);
            root = newRoot;
        }
    }

    @Override
    public void delete(IndexEntry entry) {

    }
}
