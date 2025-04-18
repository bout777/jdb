package com.jdb.storage;

import com.jdb.common.PageHelper;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.jdb.common.Constants.PAGE_SIZE;

public class Disk {
    //懒加载单例，测试用
    private static Disk disk;
    public synchronized static Disk getInstance() {
        if (disk == null) {
            disk = new Disk("./data");
            disk.newFile(777,"test.db");
            disk.newFile(369,"log");
        }
        return disk;
    }

    RandomAccessFile file;

    // fid<->filename
    private FileMap fileMap = new FileMap();

    private class FileMap {
        private Map<Integer, String> fid2name = new HashMap<>();
        private Map<String, Integer> name2fid = new HashMap<>();

        public String getName(int fid) {
            String name = fid2name.get(fid);
            if (name == null)
                throw new NoSuchElementException("no such file");
            return name;
        }

        public int getFid(String fileName) {
            Integer fid = name2fid.get(fileName);
            if (fid == null)
                throw new NoSuchElementException("no such file");
            return fid;
        }

        public void put(int fid, String fileName) {
            fid2name.put(fid, fileName);
            name2fid.put(fileName, fid);
        }

        public boolean contains(int fid) {
            return fid2name.containsKey(fid);
        }

        public boolean contains(String fileName) {
            return name2fid.containsKey(fileName);
        }

        public void clear() {
            fid2name.clear();
            name2fid.clear();
        }
    }

    // filename->nextPageId
    private final Map<String, Long> nextPage = new HashMap<>();

    public Disk(String path) {
    }



    //页面在磁盘中以pageId连续存储
    public void readPage(String path, int pno, byte[] data) {
        try {
            file = new RandomAccessFile(path, "r");
            int pos = pno * PAGE_SIZE;
            if (pos > file.length())
                throw new NoSuchElementException("pno out of range");
            file.seek((long) pno * PAGE_SIZE);
            file.read(data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Page readPage(long pid) {
        int fid = PageHelper.getFid(pid);
        int pno = PageHelper.getPno(pid);
        Page page = new Page();
        page.pid = pid;
        readPage(fid, pno, page.getData());
        return page;
    }

    public void readPage(int fid, int pno, byte[] data) {
        String path = fileMap.getName(fid);
        readPage(path, pno, data);
    }

    public void writePage(String path, int pno, byte[] data) {
        try {
            file = new RandomAccessFile(path, "rw");
            file.seek((long) pno * PAGE_SIZE);
            file.write(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void writePage(int fid, int pno, byte[] data) {
        String path = fileMap.getName(fid);
        writePage(path, pno, data);
    }

    public void writePage(Page page) {
        int fid = PageHelper.getFid(page.pid);
        int pno = PageHelper.getPno(page.pid);
        writePage(fid, pno, page.getData());
    }

    public long getNextPageIdAndIncrease(int fid) {
        String path = fileMap.getName(fid);
        long pid = nextPage.getOrDefault(path, PageHelper.concatPid(fid, 0));
        nextPage.put(path, pid + 1);
        return pid;
    }

    public long getNextPageIdAndIncrease(String path) {
        int fid = fileMap.getFid(path);
        long pid = nextPage.getOrDefault(path, PageHelper.concatPid(fid, 0));
        nextPage.put(path, pid + 1);
        return pid;
    }


    public int newFile(int fid, String fileName) {
        fileMap.put(fid, fileName);
        return fid;
    }
}
