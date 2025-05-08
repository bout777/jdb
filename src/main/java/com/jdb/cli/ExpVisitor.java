package com.jdb.cli;

import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;

public class ExpVisitor extends ExpressionVisitorAdapter {
    public ExpVisitor() {
        super();
    }


    @Override
    public void visit(OrExpression expr) {

    }

    @Override
    public void visit(AndExpression expr) {

    }

    @Override
    public void visit(MinorThan expr) {

    }

    @Override
    public void visit(MinorThanEquals expr) {

    }

    @Override
    public void visit(GreaterThan expr) {
    }

    @Override
    public void visit(GreaterThanEquals expr) {
    }

    @Override
    public void visit(EqualsTo expr) {
    }
}
