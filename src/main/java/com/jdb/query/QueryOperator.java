package com.jdb.query;

import com.jdb.catalog.Schema;
import com.jdb.table.RowData;

import java.util.Iterator;

public abstract class QueryOperator {
    protected Schema outSchema;
    public abstract Iterator<RowData> iterator();
    protected OperatorType type;
    protected QueryOperator source;
    public enum OperatorType {
        PROJECT,
        FILTER,
        SEQ_SCAN,
        INDEX_SCAN,
        JOIN,
        SELECT,
        GROUP_BY,
        SORT,
        LIMIT,
        MATERIALIZE
    }
    protected QueryOperator(OperatorType type) {
        this.type = type;
    }
    protected QueryOperator(OperatorType type,QueryOperator source){
        this.type = type;
        this.source = source;
        outSchema = computeSchema();
    }
    public Schema getSchema() {
        return outSchema;
    }

    public void setSchema(Schema schema) {
        outSchema = schema;
    }

    public abstract Schema computeSchema();
}
