package com.jdb.storage;


import com.jdb.common.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class BufferPool {
    private static volatile BufferPool instance;
    /*
     * 为了实现单表暂时这样写
     * 后续要根据表空间来分配
     * 缓冲池内应该有多张表的页*/
    private final Disk disk;
    private final Map<Long, Page> buffers;
    // 临时修改，为了配合日志文件的测试
//    private final Map<Long, Page> buf = new HashMap<>();

    // filename->nextPageId
    private final Map<String, Long> nextPage = new HashMap<>();

    // fid<->filename
    private FileMap fileMap = new FileMap();
    private class FileMap {
        private Map<Integer,String> fid2name = new HashMap<>();
        private Map<String,Integer> name2fid = new HashMap<>();
        public String getName(int fid){
            String name = fid2name.get(fid);
            if(name==null)
                throw new NoSuchElementException("no such file");
            return name;
        }
        public int getFid(String fileName){
            Integer fid= name2fid.get(fileName);
            if(fid==null)
                throw new NoSuchElementException("no such file");
            return fid;
        }
        public void put(int fid,String fileName){
            fid2name.put(fid,fileName);
            name2fid.put(fileName,fid);
        }

        public boolean contains(int fid){
            return fid2name.containsKey(fid);
        }

        public boolean contains(String fileName){
            return name2fid.containsKey(fileName);
        }

        public void clear(){
            fid2name.clear();
            name2fid.clear();
        }
    }

    private BufferPool(Disk disk) {
        buffers = new ConcurrentHashMap<>();
        this.disk = disk;
    }

    public static BufferPool getInstance() {
        if (instance == null) {
            synchronized (BufferPool.class) {
                if (instance == null) {
                    instance = new BufferPool(new Disk());
                }
            }
        }


        return instance;
    }

    public int newFile(int fid,String fileName){
        fileMap.put(fid,fileName);
        return fid;
    }


    public Page getPage(long pid) {
        //todo 添加pid是否合法的检查，（页面对应文件是否存在，文件已分配页数是否大于该页pno）
        Page page = buffers.get(pid);
        int pno = getPno(pid);

        String name = fileMap.getName(getFid(pid));
        if (page == null) {
            page = new Page();
            disk.readPage(name, pno, page.getData());
        } else {
            return page;
        }
        buffers.put(pid, page);
        return page;
    }



    public Page newPage(String fileName) {
        int fid = fileMap.getFid(fileName);
        Page page = new Page();
        page.pid = nextPage.getOrDefault(fileName,Utils.concatPid(fid,0));
        nextPage.put(fileName, page.pid + 1);
        buffers.put(page.pid, page);
        return page;
    }


    public Page newPage(int fid){
        String fileName = fileMap.getName(fid);
        Page page = new Page();
        page.pid = nextPage.getOrDefault(fileName, Utils.concatPid(fid,0));
        nextPage.put(fileName, page.pid + 1);
        buffers.put(page.pid, page);
        return page;
    }

    public void flush() {
        Set<Map.Entry<Long, Page>> entries = buffers.entrySet();
        for (Map.Entry<Long, Page> entry : entries) {
            Page page = entry.getValue();
            int pno = getPno(entry.getKey());
            if (page.isDirty()) {
                disk.writePage("test.db",pno, page.getData());
            }
        }
    }

    private int getFid(long pid){
        return (int) (pid>>32);
    }

    private int getPno(long pid){
        return (int) (pid & 0xffffffffL);
    }

    public void flushPage(long pid) {
        int fid = getFid(pid);
        int pno = getPno(pid);
        Page page = buffers.get(pid);
        if (page == null)
            throw new RuntimeException("page not found");
        disk.writePage(fileMap.getName(fid),pno, page.getData());
        page.setDirty(false);
    }

    public void shutdown() {
        buffers.clear();
    }

}
