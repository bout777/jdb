package com.idme.index;

import com.idme.common.Value;
import com.idme.table.Page;
import com.idme.table.PagePointer;
import com.idme.table.Record;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public abstract class Node {
    static final int ORDER = 4;
    public int pageId;
    Page page;

    List<Value<?>> keys;

    public abstract PagePointer search(Value<?> key);
    public abstract Node insert(IndexEntry entry);
    public abstract Value<?> getMinKey();
}

class InnerNode extends Node {
    List<Node> children;

    public InnerNode(int pageId, Page page, List<IndexEntry> entries, List<Node> children) {
        this.pageId = pageId;
        this.page = page;

    }


    @Override
    public PagePointer search(Value<?> key) {
            int i = findChild(key);
            return children.get(i).search(key);
    }

    @Override
    public Node insert(IndexEntry entry) {
        int childIndex = findChild(entry.getKey());
        Node child = children.get(childIndex);

        Node newChile = child.insert(entry);
        if(newChile == null){
            return null;
        }

        //子节点分裂后，将新节点最小键和指针加入当前节点
        keys.add(childIndex,newChile.getMinKey());
        children.add(childIndex+1,newChile);

        if(keys.size() >= ORDER){
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

    private InnerNode split() {
//        InnerNode newInnerNode = new InnerNode();
//
//        int splitIndex = keys.size()/2;
//
//        newInnerNode.keys = new ArrayList<>(keys.subList(splitIndex,keys.size()));
//        keys = new ArrayList<>(keys.subList(0,splitIndex));
//
//        newInnerNode.children = new ArrayList<>(children.subList(splitIndex,children.size()));
//        children = new ArrayList<>(children.subList(0,splitIndex));
//
//        return newInnerNode;
        return null;
    }

    private int findChild(Value<?> key) {
        int i = Collections.binarySearch(keys, key);
        return 0;
    }
}

class LeafNode extends Node {
    private List<IndexEntry> entries;
    private LeafNode nextLeaf;

    @Override
    public PagePointer search(Value<?> key) {
        //二分查找record
        return null;
    }

    @Override
    public Node insert(IndexEntry entry) {
        int insertIndex = Collections.binarySearch(keys, entry.getKey());
        //暂时只能插入唯一键
        if(insertIndex>=0){
            throw new RuntimeException("key already exist");
        }
        insertIndex = -insertIndex-1;
        entries.add(insertIndex,entry);

        if(entries.size() >= ORDER){
            return split();
        }

        return null;
    }

    @Override
    public Value<?> getMinKey() {
        return entries.get(0).getKey();
    }

    private LeafNode split() {
        LeafNode newLeaf = new LeafNode();

        int splitIndex = entries.size()/2;

        newLeaf.entries = new ArrayList<>(entries.subList(splitIndex,entries.size()));
        entries = new ArrayList<>(entries.subList(0,splitIndex));

        newLeaf.nextLeaf = this.nextLeaf;
        this.nextLeaf = newLeaf;

        return newLeaf;
    }
}
