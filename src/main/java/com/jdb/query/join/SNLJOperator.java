package com.jdb.query.join;

import com.jdb.Engine;
import com.jdb.query.JoinOperator;
import com.jdb.query.QueryOperator;
import com.jdb.table.RowData;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class SNLJOperator extends JoinOperator {

    protected SNLJOperator(QueryOperator leftSource,
                           QueryOperator rightSource,
                           String leftColumnName,
                           String rightColumnName,
                           Engine engine) {
        super(leftSource,
                rightSource,
                leftColumnName,
                rightColumnName,
                engine,
                JoinType.SNLJ);


    }


    @Override
    public Iterator<RowData> iterator() {
        class SNLJOperatorIterator implements Iterator<RowData> {
            private Iterator<RowData> leftIter;
            private Iterator<RowData> rightIter;
            private RowData left;
            private RowData next;

            SNLJOperatorIterator() {
                leftIter = getLeftSource().iterator();
                rightIter = getRightSource().iterator();
                if (leftIter.hasNext()) left = leftIter.next();
            }

            @Override
            public boolean hasNext() {
                if (next != null) return true;
                if (left == null) return false;
                while (true) {
                    if (rightIter.hasNext()) {
                        var right = rightIter.next();
                        if (compare(left, right) == 0) {
                            next = left.concat(right);
                            return true;
                        }
                    } else if (leftIter.hasNext()) {
                        left = leftIter.next();
                        rightIter = getRightSource().iterator();
                    } else {
                        left  = null;
                        return false;
                    }
                }
            }

            @Override
            public RowData next() {
                if (!hasNext()) throw new NoSuchElementException();
                var row = next;
                next = null;
                return row;
            }
        }
        return new SNLJOperatorIterator();
    }
}
