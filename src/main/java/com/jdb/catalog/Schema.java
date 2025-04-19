package com.jdb.catalog;

import com.jdb.common.DataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Schema {

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

    public Column get(int i) {
        return columns.get(i);
    }
}
