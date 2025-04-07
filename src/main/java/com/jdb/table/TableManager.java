package com.jdb.table;

public class TableManager {
    private static TableManager instance = new TableManager();
    public static TableManager getInstance() {
        return instance;
    }
    public String getTableName(int fid) {
        return "test";
    }
}
