package com.jdb.query;

import com.jdb.catalog.Schema;
import com.jdb.common.PredicateOperator;
import com.jdb.common.value.Value;
import com.jdb.table.RowData;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

public class SelectOperator extends QueryOperator{
    int columnIndex;
    PredicateOperator op;
    Value value;

    @Override
    public Schema computeSchema() {
        return this.source.getSchema();
    }

    @Override
    public Iterator<RowData> iterator() {
        class SelectOperatorIterator implements Iterator<RowData>{
            private Iterator<RowData> sourceIter;
            private RowData nextRow;
            SelectOperatorIterator(){
                sourceIter = source.iterator();
            }
            @Override
            public boolean hasNext() {
                if (nextRow != null){
                    return true;
                }
                while(sourceIter.hasNext()){
                    var rowData = sourceIter.next();
                    if (op.evaluate(rowData.values.get(columnIndex),value)){
                        nextRow = rowData;
                        return true;
                    }
                }
                return false;
            }

            @Override
            public RowData next() {
               if(hasNext()){
                   var rowData = nextRow;
                   nextRow = null;
                   return rowData;
               }
               throw new NoSuchElementException();
            }
        }
        return new SelectOperatorIterator();
    }

    public SelectOperator(QueryOperator source,
                          String columnName,
                          PredicateOperator op,
                          Value value){
        super(OperatorType.SELECT,source);
         this.columnIndex= getSchema().getColumnIndex(columnName);
         this.op = op;
         this.value = value;
    }


}
