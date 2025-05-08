package com.jdb.query;

import com.jdb.catalog.Schema;
import com.jdb.common.value.BoolValue;
import com.jdb.common.value.Value;
import com.jdb.query.expr.Expression;
import com.jdb.table.RowData;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class ExprFilterOperator extends QueryOperator{
    Expression expression;

    public ExprFilterOperator(QueryOperator source,Expression expression) {
        super(OperatorType.FILTER, source);
        this.expression = expression;
    }

    @Override
    public Iterator<RowData> iterator() {
        class ExprFilterIterator implements Iterator<RowData>{
            private Iterator<RowData> sourceIter;
            private RowData next;
            @Override
            public boolean hasNext() {
                if (next != null){
                    return true;
                }
                while(sourceIter.hasNext()){
                    var rowData = sourceIter.next();
                    var b = (BoolValue)expression.evaluate(rowData);
                    if (b.equals(Value.of(true))){
                        next = rowData;
                        return true;
                    }
                }
                return false;
            }

            @Override
            public RowData next() {
                if(hasNext()){
                    var row = next;
                    next = null;
                    return row;
                }
                throw new NoSuchElementException();
            }
        }
        return new ExprFilterIterator();
    }

    @Override
    public Schema computeSchema() {
        return this.source.getSchema();
    }
}
