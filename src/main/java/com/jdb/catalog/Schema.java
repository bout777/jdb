package com.jdb.catalog;

import com.jdb.common.DataType;

import java.nio.ByteBuffer;
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

    public Schema add(DataType type, String name) {
        return add(new Column(type, name));
    }

    public Column get(int i) {
        return columns.get(i);
    }

    public Column get(String name) {
        if(!columnsMap.containsKey(name)){
            throw new IllegalArgumentException("column not found");
        }
        return columnsMap.get(name);
    }

    public int getColumnIndex(String name) {
        Column column = columnsMap.get(name);
        return columns.indexOf(column);
    }



    public static Schema fromString(String str) {
        Schema schema = new Schema();
        // 示例输入："Schema{columns=[Column{type=INT, name='id'}, Column{type=VARCHAR, name='name'}]}"
        String columnsStr = str.substring(str.indexOf('[') + 1, str.lastIndexOf(']'));

        if (columnsStr.isEmpty()) return schema;

        String[] columnEntries = columnsStr.split("(?<=}), "); // 使用正向预查分割列条目

        for (String entry : columnEntries) {
            // 解析数据类型
            int typeStart = entry.indexOf("type=") + 5;
            int typeEnd = entry.indexOf(',', typeStart);
            DataType type = DataType.valueOf(entry.substring(typeStart, typeEnd));

            // 解析列名
            int nameStart = entry.indexOf("name='") + 6;
            int nameEnd = entry.lastIndexOf('\'');
            String name = entry.substring(nameStart, nameEnd);

            schema.add(new Column(type, name));
        }
        return schema;
    }

    public Schema concat(Schema other) {
        Schema merged = new Schema();

        // 添加当前Schema的所有列
        for (Column col : this.columns) {
            String colName = col.getName();
            if (merged.columnsMap.containsKey(colName)) {
                throw new IllegalArgumentException("Duplicate column name in current schema: " + colName);
            }
            merged.add(col);
        }

        // 添加另一个Schema的所有列
        for (Column col : other.columns) {
            String colName = col.getName();
            if (merged.columnsMap.containsKey(colName)) {
                throw new IllegalArgumentException("Duplicate column name in other schema: " + colName);
            }
            merged.add(col);
        }

        return merged;
    }


    @Override
    public String toString() {
        return "Schema{" +
                "columns=" + columns +
                '}';
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Schema that = (Schema) o;
        return columns.equals(that.columns);
    }


}
