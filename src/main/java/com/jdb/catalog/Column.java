package com.jdb.catalog;

import com.jdb.common.DataType;

public class Column {
    DataType type;
    String name;
    int Bytes;
    int length;

    public Column(DataType type, String name) {
        this.type = type;
        this.name = name;
        this.Bytes = Bytes;
    }

    public String getName() {
        return name;
    }

    public DataType getType() {
        return type;
    }
}
