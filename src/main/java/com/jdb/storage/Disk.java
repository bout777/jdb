package com.jdb.storage;

import com.jdb.Engine;
import com.jdb.common.PageHelper;
import com.jdb.common.Value;
import com.jdb.recovery.RecoveryManager;
import com.jdb.table.RowData;
import com.jdb.table.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.jdb.common.Constants.*;

public class Disk {
    //懒加载单例，测试用
//    private static Disk disk;
//
//    public synchronized static Disk getInstance() {
//        if (disk == null) {
////            disk = new Disk("./data");
////            disk.putFile(777, "test.db");
////            disk.putFile(369, "log");
//        }
//        return disk;
//    }

    private Engine engine;

    private Table fileTable;

    private int nextFid = 100;

    private String dbDir;

    private RecoveryManager recoveryManager;

    // fid->jbfile
    private final Map<Integer, JBFile> files = new HashMap<>();

    // fid->nextPageId
    private final Map<Integer, Long> nextPage = new HashMap<>();

    //======init method======//
    public Disk(String dbDir) {
        this.dbDir = dbDir;
    }

    public Disk(Engine engine){
        this.engine = engine;
    }

    public void injectDependency() {
        this.recoveryManager = engine.getRecoveryManager();
        this.dbDir = engine.getDir();
    }

    public void setFileTable(Table fileTable) {
        this.fileTable = fileTable;
    }

    public void setRecoveryManager(RecoveryManager rm) {
        this.recoveryManager = rm;
    }

    public void load() {
        var iter = fileTable.scan();
        while (iter.hasNext()) {
            var rowData = iter.next();
            int fid = (int) rowData.values.get(0).getValue(Integer.class);
            String fileName = (String) rowData.values.get(1).getValue(String.class);
            putFileMap(fid, fileName);
            nextFid = fid+1;
        }
    }

    public void init() {

    }


    //======page api======//
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

    public void writePage(Page page) {
        int fid = PageHelper.getFid(page.pid);
        int pno = PageHelper.getPno(page.pid);
        writePage(fid, pno, page.getData());
    }

    private void writePage(int fid, int pno, byte[] data) {
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

    public Page allocPage(int fid) {
        long pid = getNextPageIdAndIncrease(fid);

        recoveryManager.logPageAlloc(pid);

        return new Page(pid);
    }

    private long getNextPageIdAndIncrease(int fid) {
        long pid = nextPage.getOrDefault(fid, PageHelper.concatPid(fid, 0));
        nextPage.put(fid, pid + 1);
        return pid;
    }


    //======file api======//

    public int addFile(String fileName){
        int fid = putFileTable(fileName);
        putFileMap(fid, fileName);
        return fid;
    }

    private void putFileMap(int fid, String fileName){
//        if (files.containsKey(fid))
//            throw new RuntimeException("file already exists");
        var file = new JBFile(dbDir + "/" + fileName);
        files.put(fid, file);
        nextPage.put(fid, PageHelper.concatPid(fid,(int)file.length() / PAGE_SIZE));
    }

    private int putFileTable(String fileName){
        int fid = nextFid++;

        List<Value> values = new ArrayList<>();
        values.add(Value.of(fid));
        values.add(Value.of(fileName));

        fileTable.insertRecord(new RowData(values), true, false);

        return fid;
    }

    public void close() {
        for (JBFile file : files.values()) {
            try {
                file.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        files.clear();
        nextPage.clear();
    }
}
