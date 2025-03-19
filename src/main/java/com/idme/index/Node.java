package com.idme.index;

import com.idme.common.Value;
import com.idme.storage.BufferPool;
import com.idme.storage.Page;
import com.idme.table.DataPage;
import com.idme.table.PagePointer;
import com.idme.table.Slot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
    //不应该保存对页对象的引用，而是从BufferPool随用随取，否则会造成大量内存无法回收，
//    Page page;
    List<Value<?>> keys;

    Node() {
        keys = new ArrayList<>();
    }

    public abstract PagePointer search(Value<?> key);

    public abstract Node insert(IndexEntry entry);

    public abstract Value<?> getMinKey();

    public abstract void readFromPage(int pageId);

    public abstract void write2Page();
}

class InnerNode extends Node {
    List<Node> children;

    public InnerNode() {
        super();
        children = new ArrayList<>();
    }

    @Override
    public PagePointer search(Value<?> key) {
        int i = Collections.binarySearch(keys, key);
        if (i >= 0)
            return children.get(i + 1).search(key);
        else return children.get(-i - 1).search(key);

    }

    @Override
    public Node insert(IndexEntry entry) {
        int childIndex = findChild(entry.getKey());
        Node child = children.get(childIndex);

        Node newChile = child.insert(entry);
        if (newChile == null) {
            return null;
        }

        //子节点分裂后，将新节点最小键和指针加入当前节点
        keys.add(childIndex, newChile.getMinKey());
        children.add(childIndex + 1, newChile);

        if (keys.size() >= ORDER) {
            //分裂内部节点
            return split();
        }

        //无需分裂
        return null;
    }

    @Override
    public Value<?> getMinKey() {
        return children.get(0).getMinKey();
    }

    @Override
    public void readFromPage(int pageId) {
        //TODO

    }

    @Override
    public void write2Page() {

    }

    private InnerNode split() {
        InnerNode newNode = new InnerNode();
        int splitIndex = keys.size() / 2;

        newNode.keys = new ArrayList<>(keys.subList(splitIndex + 1, keys.size()));
        newNode.children = new ArrayList<>(children.subList(splitIndex + 1, children.size()));

        keys = new ArrayList<>(keys.subList(0, splitIndex));
        children = new ArrayList<>(children.subList(0, splitIndex + 1));

        return newNode;
    }

    private int findChild(Value<?> key) {
        int i = Collections.binarySearch(keys, key);
        if (i >= 0) {
            return i;
//            throw new RuntimeException("key already exist");
        }
        return -i - 1;
    }
}

class LeafNode extends Node {
    private List<IndexEntry> entries;
    private LeafNode nextLeaf;

    LeafNode() {
        super();
        entries = new ArrayList<>();
    }

    @Override
    public PagePointer search(Value<?> key) {
        IndexEntry e = new IndexEntry(key, null);
        int i = Collections.binarySearch(entries, e);
        if (i >= 0)
            return entries.get(i).pointer;
        else
            throw new RuntimeException("key not found");
    }

    @Override
    public Node insert(IndexEntry entry) {
        int insertIndex = Collections.binarySearch(entries, entry);
        //暂时只能插入唯一键
        if (insertIndex >= 0) {
            throw new RuntimeException("key already exist");
        }
        insertIndex = -insertIndex - 1;
        entries.add(insertIndex, entry);

        if (entries.size() >= ORDER) {
            return split();
        }

        return null;
    }

    @Override
    public Value<?> getMinKey() {
        return entries.get(0).getKey();
    }

    @Override
    public void readFromPage(int pageId) {
        Page page = BufferPool.getInstance().getPage(pageId);
        DataPage dataPage = new DataPage(pageId,page);
        int n = dataPage.getRecordCount();
        for (int i = 0; i < n; i++) {
            Slot slot = dataPage.getSlot(i);
            entries.add(new IndexEntry(Value.ofInt(slot.getPrimaryKey()), new PagePointer(pageId, i)));
        }
    }

    @Override
    public void write2Page() {

    }

    private LeafNode split() {
        LeafNode newLeaf = new LeafNode();

        int splitIndex = entries.size() / 2;

        newLeaf.entries = new ArrayList<>(entries.subList(splitIndex, entries.size()));
        entries = new ArrayList<>(entries.subList(0, splitIndex));

        newLeaf.nextLeaf = this.nextLeaf;
        this.nextLeaf = newLeaf;

        return newLeaf;
    }
}
