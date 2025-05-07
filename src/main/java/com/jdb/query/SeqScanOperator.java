package com.jdb.query;

import com.jdb.Engine;
import com.jdb.catalog.Schema;
import com.jdb.table.RowData;
import com.jdb.table.Table;

import java.util.Iterator;

public class SeqScanOperator extends QueryOperator{
    Iterator<RowData> iterator;
    Table table;
    @Override
    public Iterator<RowData> iterator() {
        return table.scan();
    }

    @Override
    public Schema computeSchema() {
        return getSchema();
    }

    public SeqScanOperator(String tableName, Engine engine){
        super(OperatorType.SEQ_SCAN);
        table = engine.getTableManager().getTable(tableName);
    }
}
