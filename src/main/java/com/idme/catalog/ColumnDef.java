package com.idme.catalog;

import com.idme.common.DataType;

public class ColumnDef {
    DataType type;
    String name;
    int Bytes;
    int length;
    public ColumnDef(DataType type, String name){
        this.type = type;
        this.name = name;
        this.Bytes = Bytes;
    }
    public String getName(){
        return name;
    }
    public DataType getType(){
        return type;
    }
}
