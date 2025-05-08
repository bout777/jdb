package com.jdb.storage;

import com.jdb.Engine;
import com.jdb.common.PageHelper;
import com.jdb.common.value.Value;
import com.jdb.exception.DuplicateInsertException;
import com.jdb.recovery.RecoveryManager;
import com.jdb.table.RowData;
import com.jdb.table.Table;
import com.jdb.transaction.TransactionContext;

import java.io.IOException;
import java.util.*;

import static com.jdb.common.Constants.*;

public class Disk {
    // fid->jbfile
    private final Map<Integer, JBFile> files = new HashMap<>();
    // fid->nextPageId
    private final Map<Integer, Long> nextPage = new HashMap<>();
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
    public int ioCount = 0;
    private Engine engine;
    private Table fileTable;
    private int nextFid = 100;
    private String dbDir;
    private RecoveryManager recoveryManager;

    //======init method======//
    public Disk(String dbDir) {
        this.dbDir = dbDir;
    }

    public Disk(Engine engine) {
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
            try {
                putFileMap(fid, fileName);
            } catch (DuplicateInsertException e) {

            }
            nextFid = fid + 1;
        }
    }

    public void init() {
        putFileMap(LOG_FILE_ID, LOG_FILE_NAME);
        putFileMap(FILE_META_DATA_FILE_ID, FILE_META_DATA_FILE_NAME + TABLE_FILE_SUFFIX);
        putFileMap(TABLE_META_DATA_FILE_ID, TABLE_META_DATA_FILE_NAME + TABLE_FILE_SUFFIX);
        putFileMap(INDEX_META_DATA_FILE_ID, INDEX_META_DATA_FILE_NAME + TABLE_FILE_SUFFIX);
    }


    //======page api======//
    public Page readPage(long pid) {
        ioCount++;
        Page page = new Page(pid);
        int fid = PageHelper.getFid(pid);
        int pno = PageHelper.getPno(pid);
        readPage(fid, pno, page.getData());
        return page;
    }

    private void readPage(int fid, int pno, byte[] data) {

        JBFile file = files.get(fid);
        if (file == null)
            throw new NoSuchElementException(fid + "file no existed");
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
        ioCount++;
        int fid = PageHelper.getFid(page.pid);
        int pno = PageHelper.getPno(page.pid);
        writePage(fid, pno, page.getData());
    }

    private void writePage(int fid, int pno, byte[] data) {
        JBFile file = files.get(fid);
        if (file == null)
            throw new NoSuchElementException(fid + "file no existed");
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
        ioCount++;
        long pid = getNextPageIdAndIncrease(fid);
        return new Page(pid);
    }

    private long getNextPageIdAndIncrease(int fid) {
        if (!nextPage.containsKey(fid))
            throw new NoSuchElementException("file no existed");
        long pid = nextPage.get(fid);
        nextPage.put(fid, pid + 1);
        return pid;
    }


    //======file api======//
    public boolean fileExist(int fid) {
        return files.containsKey(fid);
    }

    public int addFile(String fileName) {
        ioCount++;
        int fid = putFileTable(fileName);
        putFileMap(fid, fileName);
        long xid = TransactionContext.getTransaction().getXid();
        recoveryManager.logCreateFile(xid, fid, fileName);
        return fid;
    }

    public void putFileMap(int fid, String fileName) {
        if (files.containsKey(fid))
            throw new DuplicateInsertException("file already exists");
        var file = new JBFile(dbDir + "/" + fileName);
        files.put(fid, file);
        nextPage.put(fid, PageHelper.concatPid(fid, (int) file.length() / PAGE_SIZE));
    }

    private int putFileTable(String fileName) {
        int fid = nextFid++;

        List<Value> values = new ArrayList<>();
        values.add(Value.of(fid));
        values.add(Value.of(fileName));

        fileTable.insertRecord(new RowData(values), true, false);

        return fid;
    }

    public void close() {
        for (JBFile file : files.values()) {
            file.wlock.lock();
            try {
                file.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                file.wlock.unlock();
            }
        }
        files.clear();
        nextPage.clear();
    }
}
