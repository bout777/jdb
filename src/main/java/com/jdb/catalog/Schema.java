package com.jdb.catalog;

import com.jdb.common.DataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Schema {
    //for test
    public static final Schema instance;

    static {
        instance = new Schema();
        instance.add(new ColumnDef(DataType.STRING, "name"));
        instance.add(new ColumnDef(DataType.INTEGER, "age"));
    }

    private List<ColumnDef> columns;
    private HashMap<String, ColumnDef> columnsMap;

    public Schema() {
        columns = new ArrayList<>();
        columnsMap = new HashMap<>();

    }

    public List<ColumnDef> columns() {
        return this.columns;
    }

    public void add(ColumnDef columnDef) {
        columns.add(columnDef);
        columnsMap.put(columnDef.getName(), columnDef);
    }
}
