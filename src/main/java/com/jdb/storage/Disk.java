package com.jdb.storage;

import com.jdb.common.PageHelper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Disk {
    //懒加载单例，测试用
    private static Disk disk;

    public synchronized static Disk getInstance() {
        if (disk == null) {
            disk = new Disk("./data");
            disk.putFile(777, "test.db");
            disk.putFile(369, "log");
        }
        return disk;
    }

    private final String path;

    // fid->jbfile
    Map<Integer, JBFile> files = new HashMap<>();


    // fid->nextPageId
    private final Map<Integer, Long> nextPage = new HashMap<>();

    public Disk(String path) {
        this.path = path;
    }

    public Page readPage(long pid) {
        Page page = new Page(pid);
        int fid = PageHelper.getFid(pid);
        int pno = PageHelper.getPno(pid);
        readPage(fid, pno, page.getData());
        return page;
    }

    private void readPage(int fid, int pno, byte[] data) {
        JBFile file = files.get(fid);
        try {
            file.rlock.lock();
            file.read(pno, data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            file.rlock.unlock();
        }
    }

    private void writePage(int fid, int pno, byte[] data) {
//        String path = fileMap.getName(fid);
//        writePage(path, pno, data);
        JBFile file = files.get(fid);
        try {
            file.wlock.lock();
            file.write(pno, data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            file.wlock.unlock();
        }
    }

    public void writePage(Page page) {
        int fid = PageHelper.getFid(page.pid);
        int pno = PageHelper.getPno(page.pid);
        writePage(fid, pno, page.getData());
    }

    private long getNextPageIdAndIncrease(int fid) {
        long pid = nextPage.getOrDefault(fid, PageHelper.concatPid(fid, 0));
        nextPage.put(fid, pid + 1);
        return pid;
    }

    public Page allocPage(int fid) {
        //todo rm记录日志
        long pid = getNextPageIdAndIncrease(fid);
        return new Page(pid);
    }

    public void putFile(int fid, String fileName) {
        if (files.containsKey(fid))
            throw new RuntimeException("file already exists");
        files.put(fid, new JBFile(path + "/" + fileName));
    }
}
