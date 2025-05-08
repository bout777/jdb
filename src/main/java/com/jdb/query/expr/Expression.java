package com.jdb.query.expr;

import com.jdb.common.value.BoolValue;
import com.jdb.common.value.Value;
import com.jdb.table.RowData;

public abstract class Expression {
    /**
     * Evaluate this expression against the given row data.
     */
    public abstract Value evaluate(RowData rowData);

    /**
     * Convert this expression to a human-readable string.
     */
    @Override
    public abstract String toString();

    static class AndExpression extends Expression {
        private final Expression left;
        private final Expression right;

        public AndExpression(Expression left, Expression right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public Value evaluate(RowData rowData) {
            var l = (BoolValue) left.evaluate(rowData);
            var r = (BoolValue) right.evaluate(rowData);
            return l.and(r);
        }

        @Override
        public String toString() {
            return "(" + left + " AND " + right + ")";
        }
    }

    static class OrExpression extends Expression {
        private final Expression left;
        private final Expression right;

        public OrExpression(Expression left, Expression right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public Value evaluate(RowData rowData) {
            var l = (BoolValue) left.evaluate(rowData);
            var r = (BoolValue) right.evaluate(rowData);
            return l.or(r);
        }

        @Override
        public String toString() {
            return "(" + left + " OR " + right + ")";
        }
    }

    static class NotExpression extends Expression {
        private final Expression expression;

        public NotExpression(Expression expression) {
            this.expression = expression;
        }

        @Override
        public Value evaluate(RowData rowData) {
            var e = (BoolValue) expression.evaluate(rowData);
            return e.not();
        }

        @Override
        public String toString() {
            return "(NOT " + expression + ")";
        }
    }

    static class GraterThanExpression extends Expression {
        private final Expression left;
        private final Expression right;

        public GraterThanExpression(Expression left, Expression right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public Value evaluate(RowData rowData) {
            var l = left.evaluate(rowData);
            var r = right.evaluate(rowData);
            return Value.of(l.compareTo(r) > 0);
        }

        @Override
        public String toString() {
            return "(" + left + " > " + right + ")";
        }
    }

    static class GraterThanEqualsExpression extends Expression {
        private final Expression left;
        private final Expression right;

        public GraterThanEqualsExpression(Expression left, Expression right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public Value evaluate(RowData rowData) {
            var l = left.evaluate(rowData);
            var r = right.evaluate(rowData);
            return Value.of(l.compareTo(r) >= 0);
        }

        @Override
        public String toString() {
            return "(" + left + " >= " + right + ")";
        }
    }

    static class LessThanExpression extends Expression {
        private final Expression left;
        private final Expression right;

        public LessThanExpression(Expression left, Expression right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public Value evaluate(RowData rowData) {
            var l = left.evaluate(rowData);
            var r = right.evaluate(rowData);
            return Value.of(l.compareTo(r) < 0);
        }

        @Override
        public String toString() {
            return "(" + left + " < " + right + ")";
        }
    }

    static class LessThanEqualsExpression extends Expression {
        private final Expression left;
        private final Expression right;

        public LessThanEqualsExpression(Expression left, Expression right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public Value evaluate(RowData rowData) {
            var l = left.evaluate(rowData);
            var r = right.evaluate(rowData);
            return Value.of(l.compareTo(r) <= 0);
        }

        @Override
        public String toString() {
            return "(" + left + " <= " + right + ")";
        }
    }

    static class EqualsExpression extends Expression {
        private final Expression left;
        private final Expression right;

        public EqualsExpression(Expression left, Expression right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public Value evaluate(RowData rowData) {
            var l = left.evaluate(rowData);
            var r = right.evaluate(rowData);
            return Value.of(l.compareTo(r) == 0);
        }

        @Override
        public String toString() {
            return "(" + left + " == " + right + ")";
        }
    }

    static class NotEqualsExpression extends Expression {
        private final Expression left;
        private final Expression right;

        public NotEqualsExpression(Expression left, Expression right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public Value evaluate(RowData rowData) {
            var l = left.evaluate(rowData);
            var r = right.evaluate(rowData);
            return Value.of(l.compareTo(r) != 0);
        }

        @Override
        public String toString() {
            return "(" + left + " != " + right + ")";
        }
    }

    static class Column extends Expression {
        private final String columnName;
        private final int columnIndex;

        public Column(String columnName, int columnIndex) {
            this.columnName = columnName;
            this.columnIndex = columnIndex;
        }

        @Override
        public Value evaluate(RowData rowData) {
            return rowData.get(columnIndex);
        }

        @Override
        public String toString() {
            return columnName;
        }
    }

    static class Literal extends Expression {

        private final Value value;

        public Literal(Value value) {
            this.value = value;
        }

        @Override
        public Value evaluate(RowData rowData) {
            return value;
        }

        @Override
        public String toString() {
            return value.toString();
        }
    }
}
