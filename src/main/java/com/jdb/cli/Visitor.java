package com.jdb.cli;

import com.jdb.Engine;
import com.jdb.catalog.Schema;
import com.jdb.common.DataType;
import com.jdb.common.value.Value;
import com.jdb.table.RowData;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.StatementVisitorAdapter;
import net.sf.jsqlparser.statement.UnsupportedStatement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.update.Update;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

public class Visitor extends StatementVisitorAdapter {
    Engine engine;
    InputStream in;
    PrintStream out;

    public Visitor(Engine engine, InputStream in, PrintStream out) {
        this.engine = engine;
        this.in = in;
        this.out = out;
    }



    @Override
    public void visit(Insert insert) {
        ExpressionList exprList = insert.getItemsList(ExpressionList.class);
        List<Expression> exprs = exprList.getExpressions();

        // 将每个 Expression 转为 Value<?>（需自定义 Value.of）
        List<Value> vals = exprs.stream()
                .map(expr -> Value.fromString(expr.toString()))
                .collect(Collectors.toList());

//        // 列名列表
//        List<String> cols = insert.getColumns()
//                .stream()
//                .map(Object::toString)
//                .toList();

        var rowData = new RowData(vals);
        String tableName = insert.getTable().getName();
        engine.insert(tableName, rowData);
//        out.printf();
    }

    @Override
    public void visit(Update update) {
    }


    @Override
    public void visit(Delete delete) {
    }

    @Override
    public void visit(Select select) {
        SelectBody body = select.getSelectBody();
    }

    @Override
    public void visit(CreateTable createTable) {
        var coldefs = createTable.getColumnDefinitions();
        Schema schema = new Schema();
        for(var coldef : coldefs){
            String name = coldef.getColumnName();
            DataType type = DataType.fromString(coldef.getColDataType().getDataType());
            schema.add(type, name);
        }
        String tableName = createTable.getTable().getName();
        engine.createTable(tableName, schema);
    }

    @Override
    public void visit(Drop drop) {
    }

    @Override
    public void visit(UnsupportedStatement stmt) {
    }


}
