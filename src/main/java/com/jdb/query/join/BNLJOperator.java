package com.jdb.query.join;

import com.jdb.query.JoinOperator;
import com.jdb.table.RowData;

import java.util.Iterator;

public class BNLJOperator extends JoinOperator {


    protected BNLJOperator() {
        super(JoinType.BNLJ);
    }

    @Override
    public Iterator<RowData> iterator() {
        return null;
    }
}
