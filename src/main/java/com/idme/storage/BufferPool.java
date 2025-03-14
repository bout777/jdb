package com.idme.storage;

import com.idme.table.Page;

import java.util.HashMap;

public class BufferPool {
    private Disk disk;
    private HashMap<Integer,Page> buffers;

    public BufferPool(Disk disk) {
        buffers = new HashMap<Integer,Page>();
        this.disk = disk;
    }
     public Page getPage(int pageId) {
        Page page= buffers.get(pageId);
        if(page == null){
            page = new Page(pageId);
            disk.readPage("test.db",pageId,page.getData());
        }else {
            return page;
        }
        return page;
     }

     public Page newPage(int pageId){
        Page page = new Page(pageId);
        buffers.put(pageId,page);
        return page;
     }

     public void flush(){
        for(Page page : buffers.values()){
            if(page.isDirty()){
                disk.writePage("test.db",page.pageId,page.getData());
            }
        }
     }

    void flushPage(){

    }


}
