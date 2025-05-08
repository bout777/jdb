package com.jdb.query;

import com.jdb.Engine;
import com.jdb.catalog.Schema;
import com.jdb.common.PredicateOperator;
import com.jdb.common.value.Value;
import com.jdb.table.RowData;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class IndexScanOperator extends QueryOperator {
    private final String tableName;
    private final String columnName;
    private final PredicateOperator predicate;
    private final Value value;
    private final Engine engine;
    private final int columnIndex;

    public IndexScanOperator(String tableName,
                             String columnName,
                             PredicateOperator predicate,
                             Value value,
                             Engine engine) {
        super(OperatorType.INDEX_SCAN);
        this.tableName = tableName;
        this.columnName = columnName;
        this.predicate = predicate;
        this.value = value;
        this.engine = engine;

        columnIndex = engine.getTableManager().getTable(tableName).getSchema().getColumnIndex(columnName);
    }

    @Override
    public Iterator<RowData> iterator() {
        class IndexScanIterator implements Iterator<RowData> {
            private final Iterator<RowData> sourceIter;
            private RowData next;

            IndexScanIterator() {
                sourceIter = switch (predicate) {
                    case EQUALS -> engine.lookup(tableName, columnName, value);
                    case LESS_THAN, LESS_THAN_EQUALS -> engine.scan(tableName, columnName);
                    case GREATER_THAN, GREATER_THAN_EQUALS -> engine.scanFrom(tableName, columnName, value);
                    default -> throw new IllegalArgumentException("bad index op");
                };
            }

            @Override
            public boolean hasNext() {
                if (next != null)
                    return true;
                while (sourceIter.hasNext()) {
                    var row = sourceIter.next();
                    if (predicate.evaluate(row.get(columnIndex), value)) {
                        next = row;
                        return true;
                    }
                }
                return false;
            }

            @Override
            public RowData next() {
                if (!hasNext()) throw new NoSuchElementException();
                var row = next;
                next = null;
                return row;
            }
        }
        return new IndexScanIterator();
    }

    @Override
    public Schema computeSchema() {
        return null;
    }
}
