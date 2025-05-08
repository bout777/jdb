package com.jdb.catalog;

import com.jdb.common.DataType;

public class Column {
    DataType type;
    String name;

    public Column(DataType type, String name) {
        this.type = type;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public DataType getType() {
        return type;
    }

    @Override
    public String toString() {
        return "Column{" +
                "type=" + type +
                ", name='" + name + '\'' +
                '}';
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Column that = (Column) o;
        return type == that.type &&
                name.equals(that.name);
    }

}
