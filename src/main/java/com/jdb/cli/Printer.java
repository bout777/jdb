package com.jdb.cli;

import com.jdb.common.DataType;
import com.jdb.common.value.Value;
import com.jdb.table.RowData;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Printer {
    private final PrintStream out;

    public Printer(PrintStream out) {
        this.out = out;
    }

    public void printRowData(List<String> columnNames, Iterator<RowData> iter) {
        ArrayList<Integer> maxWidths = new ArrayList<>();
        for (String columnName : columnNames) {
            maxWidths.add(columnName.length());
        }
        List<RowData> rows = new ArrayList<>();
        while (iter.hasNext()) {
            RowData rowData = iter.next();
            rows.add(rowData);
            List<Value> fields = rowData.values;
            for (int i = 0; i < fields.size(); i++) {
                Value field = fields.get(i);
                maxWidths.set(i, Integer.max(
                        maxWidths.get(i),
                        field.toString().replace("\0", "").length()
                ));
            }
        }
        printRow(columnNames, maxWidths);
        printSeparator(maxWidths);
        for (var row : rows) {
            printRowData(row, maxWidths);
        }
        if (rows.size() != 1) {
            this.out.printf("(%d rows)\n", rows.size());
        } else {
            this.out.printf("(%d row)\n", rows.size());
        }
    }

    public void printRowData(RowData rowData, List<Integer> padding) {
        List<String> row = new ArrayList<>();
        List<Value> values = rowData.getValues();
        for (int i = 0; i < values.size(); i++) {
            Value field = values.get(i);
            String cleaned = field.toString().replace("\0", "");
            if (field.getType() == DataType.INTEGER) {
                cleaned = String.format("%" + padding.get(i) + "s", cleaned);
            }
            row.add(cleaned);
        }
        printRow(row, padding);
    }

    private void printRow(List<String> values, List<Integer> padding) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) this.out.print("|");
            String curr = values.get(i);
            if (i == values.size() - 1) {
                this.out.println(" " + curr);
                break;
            }
            this.out.printf(" %-" + padding.get(i) + "s ", curr);
        }
    }

    private void printSeparator(List<Integer> padding) {
        for (int i = 0; i < padding.size(); i++) {
            if (i > 0) this.out.print("+");
            for (int j = 0; j < padding.get(i) + 2; j++)
                this.out.print("-");
        }
        this.out.println();
    }
}
