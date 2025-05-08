package com.jdb.query;

import com.jdb.Engine;
import com.jdb.catalog.Schema;
import com.jdb.table.RowData;

public abstract class JoinOperator extends QueryOperator {
    protected JoinType joinType;
    protected int leftColumnIndex;
    protected int rightColumnIndex;
    private final QueryOperator leftSource;
    private final QueryOperator rightSource;
    private final String leftColumnName;
    private final String rightColumnName;
    private final Engine engine;

    protected JoinOperator(QueryOperator leftSource,
                           QueryOperator rightSource,
                           String leftColumnName,
                           String rightColumnName,
                           Engine engine,
                           JoinType joinType) {
        super(OperatorType.JOIN);
        this.joinType = joinType;
        this.leftSource = leftSource;
        this.rightSource = rightSource;
        this.leftColumnName = leftColumnName;
        this.rightColumnName = rightColumnName;
        this.engine = engine;
    }

    public QueryOperator getLeftSource() {
        return leftSource;
    }

    public QueryOperator getRightSource() {
        return rightSource;
    }

    @Override
    public Schema computeSchema() {
        var leftSchema = leftSource.getSchema();
        var rightSchema = rightSource.getSchema();

        this.leftColumnIndex = leftSchema.getColumnIndex(leftColumnName);
        this.rightColumnIndex = rightSchema.getColumnIndex(rightColumnName);

        return leftSchema.concat(rightSchema);
    }

    public int compare(RowData left, RowData right) {
        var leftVal = left.values.get(leftColumnIndex);
        var rightVal = right.values.get(rightColumnIndex);
        return leftVal.compareTo(rightVal);
    }

    public enum JoinType {
        SNLJ,
        PNLJ,
        BNLJ,
        SORTMERGE,
        SHJ,
        GHJ
    }
}
