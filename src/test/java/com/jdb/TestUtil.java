package com.jdb;

import com.jdb.catalog.Column;
import com.jdb.catalog.Schema;
import com.jdb.common.DataType;
import com.jdb.common.value.Value;
import com.jdb.table.RowData;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

public class TestUtil {
    public static final String TEST_DIR = "D:\\ideaProject\\jdb\\testdb";
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

    public static void cleanfile(String _path) {
        try {
            var p = Path.of(_path);
            Files.walk(p)
                    .filter(path -> !path.equals(p))
                    .sorted(Comparator.reverseOrder())
                    .forEach(file -> {
                        try {
                            Files.delete(file);
                        } catch (IOException e) {
                            throw new RuntimeException("删除失败: " + file, e);
                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
