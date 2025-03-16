package com.idme.catalog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ColumnList {
    private List<ColumnDef> columns;
    private HashMap<String,ColumnDef>columnsMap;
    public ColumnList(){
        columns = new ArrayList<>();
        columnsMap = new HashMap<>();

    }
    public List<ColumnDef> columns(){
        return this.columns;
    }
    public void add(ColumnDef columnDef){
        columns.add(columnDef);
        columnsMap.put(columnDef.getName(),columnDef);
    }
}
