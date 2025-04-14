package com.jdb.version;


import com.jdb.table.RowData;

public class ReadResult {
    public enum Visibility {
        VISIBLE,
        INVISIBLE,
        NOT_PRESENT
    }

    private Visibility visibility;
    private RowData rowData;

    public Visibility getVisibility() {
        return visibility;
    }

    public RowData getRowData() {
        return rowData;
    }

    public static ReadResult notPresent() {
        ReadResult result = new ReadResult();
        result.visibility = Visibility.NOT_PRESENT;
        return result;
    }

    public static ReadResult invisible() {
        ReadResult result = new ReadResult();
        result.visibility = Visibility.INVISIBLE;
        return result;
    }

    public static ReadResult visible(RowData rowData) {
        ReadResult result = new ReadResult();
        result.visibility = Visibility.VISIBLE;
        result.rowData = rowData;
        return result;
    }
}
