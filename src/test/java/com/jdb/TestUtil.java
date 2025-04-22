package com.jdb;

import com.jdb.catalog.Column;
import com.jdb.catalog.Schema;
import com.jdb.common.DataType;
import com.jdb.common.Value;
import com.jdb.table.RowData;
import com.jdb.table.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TestUtil {
    private static Random r = new Random();

    public static RowData generateRecord(int i) {

        List<Value> values = new ArrayList<>();

        values.add(Value.of(i));
        values.add(Value.of("hehe"));
        values.add(Value.of(r.nextInt()));

        int size = 0;
        for (Value val : values) {
            size += val.getBytes();
        }
        return new RowData(values);
    }

    public static Schema recordSchema() {
        return new Schema()
                .add(new Column(DataType.INTEGER, "id"))
                .add(new Column(DataType.STRING, "name"))
                .add(new Column(DataType.INTEGER, "age"));
    }

//    public static Table getTable() {
//        return Table.getTestTable();
//    }
}
