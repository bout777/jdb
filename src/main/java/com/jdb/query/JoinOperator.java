package com.jdb.query;

public abstract class JoinOperator extends QueryOperator{
    public enum JoinType {
        SNLJ,
        PNLJ,
        BNLJ,
        SORTMERGE,
        SHJ,
        GHJ
    }
    protected JoinType joinType;
    protected JoinOperator(JoinType joinType) {
        super(OperatorType.JOIN);
        this.joinType = joinType;
    }
}
