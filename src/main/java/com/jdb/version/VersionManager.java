package com.jdb.version;

import com.jdb.table.Record;
import com.jdb.table.RecordID;

import java.util.ArrayDeque;
import java.util.Map;

/*
* 多版本管理实现
* 读写整合到datapage里，反正日志模块已经干了，不怕再加了
* datapage读写调用vm，至于锁，再看看
*
*
*
*
* */
public class VersionManager {

    private static VersionManager instance = new VersionManager();
    public static VersionManager getInstance(){
        return instance;
    }
    private Map<String, EntryDeque> versionMap;
    public RecordID pushUpdate(int fid,RecordID rid, Record record,long xid){
        return null;
    }

    public Record read(RecordID rid){
        return null;
    }

    //都用上java了，不要省内存了，多快好省，干就完了。
    private String formatKey(String tableName,RecordID rid){
        return String.format("%s/%d/%d",tableName,rid.pid,rid.slotId);
    }

    //maintain a version list for each record
    class EntryDeque extends ArrayDeque<VersionEntry> {
        int fid;
        RecordID rid;
    }
}
