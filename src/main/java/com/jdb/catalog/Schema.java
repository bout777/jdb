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
        instance.add(new Column(DataType.STRING, "name"));
        instance.add(new Column(DataType.INTEGER, "age"));
    }

    private List<Column> columns;
    private HashMap<String, Column> columnsMap;

    public Schema() {
        columns = new ArrayList<>();
        columnsMap = new HashMap<>();

    }

    public List<Column> columns() {
        return this.columns;
    }

    public Schema add(Column column) {
        columns.add(column);
        columnsMap.put(column.getName(), column);
        return this;
    }
}
