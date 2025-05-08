package com.jdb.query.expr;

import com.jdb.Engine;
import com.jdb.catalog.Schema;
import com.jdb.common.value.Value;
import com.jdb.exception.DatabaseException;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;

public class JExpressionVisitor extends ExpressionVisitorAdapter {

    Engine engine;
    Schema schema;
    private Expression expression;

    public JExpressionVisitor(Engine engine) {
        this.engine = engine;
    }

    //用于单表查询
    public JExpressionVisitor(Schema schema) {
        this.schema = schema;
    }

//    public void setSchema(Schema schema) {
//        this.schema = schema;
//    }


    public Expression getExpression() {
        return expression;
    }

    @Override
    public void visit(AndExpression expr) {
        expr.getLeftExpression().accept(this);
        var l = expression;
        expr.getRightExpression().accept(this);
        var r = expression;
        expression = new Expression.AndExpression(l, r);
    }

    @Override
    public void visit(OrExpression expr) {
        expr.getLeftExpression().accept(this);
        var l = expression;
        expr.getRightExpression().accept(this);
        var r = expression;
        expression = new Expression.OrExpression(l, r);
    }

    @Override
    public void visit(GreaterThan expr) {
        expr.getLeftExpression().accept(this);
        var l = expression;
        expr.getRightExpression().accept(this);
        var r = expression;
        expression = new Expression.GraterThanExpression(l, r);
    }

    @Override
    public void visit(GreaterThanEquals expr) {
        expr.getLeftExpression().accept(this);
        var l = expression;
        expr.getRightExpression().accept(this);
        var r = expression;
        expression = new Expression.GraterThanEqualsExpression(l, r);
    }

    @Override
    public void visit(MinorThan expr) {
        expr.getLeftExpression().accept(this);
        var l = expression;
        expr.getRightExpression().accept(this);
        var r = expression;
        expression = new Expression.LessThanExpression(l, r);
    }

    @Override
    public void visit(MinorThanEquals expr) {
        expr.getLeftExpression().accept(this);
        var l = expression;
        expr.getRightExpression().accept(this);
        var r = expression;
        expression = new Expression.LessThanEqualsExpression(l, r);
    }

    @Override
    public void visit(EqualsTo expr) {
        expr.getLeftExpression().accept(this);
        var l = expression;
        expr.getRightExpression().accept(this);
        var r = expression;
        expression = new Expression.EqualsExpression(l, r);
    }

    @Override
    public void visit(NotEqualsTo expr) {
        expr.getLeftExpression().accept(this);
        var l = expression;
        expr.getRightExpression().accept(this);
        var r = expression;
        expression = new Expression.NotEqualsExpression(l, r);
    }

    @Override
    public void visit(Column column) {
        String name = column.getColumnName();
        int columnIndex = 0;
        try {
            columnIndex = schema.getColumnIndex(name);
        } catch (IllegalArgumentException e) {
            throw new DatabaseException(e);
        }
        expression = new Expression.Column(name, columnIndex);
    }

    @Override
    public void visit(StringValue value) {
        String v = value.getValue();
        expression = new Expression.Literal(Value.of(v));
    }

    @Override
    public void visit(LongValue value) {
        int v = (int) value.getValue();
        expression = new Expression.Literal(Value.of(v));
    }


}
