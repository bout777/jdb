package com.jdb.query;

import com.jdb.catalog.Schema;
import com.jdb.common.value.Value;
import com.jdb.table.RowData;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class ProjectOperator extends QueryOperator {
    // 需要保留的列名列表
    private List<String> outputColumns;
    // 源Schema中对应列的索引
    private List<Integer> columnIndices;

    public ProjectOperator(QueryOperator source, List<String> columns) {
        super(OperatorType.PROJECT);
        init(source, columns);
    }

    private void init(QueryOperator source, List<String> columns) {
        // 验证输入参数
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Project columns cannot be empty");
        }

        this.source = source;
        Schema sourceSchema = source.getSchema();
        this.outputColumns = new ArrayList<>(columns);
        this.columnIndices = new ArrayList<>();

        this.outSchema = new Schema();
        // 构建输出Schema并记录列索引
        for (String col : columns) {
            int index = sourceSchema.getColumnIndex(col);
            if (index == -1) {
                throw new IllegalArgumentException("Column '" + col + "' not found in source schema");
            }
            this.columnIndices.add(index);
            var type = sourceSchema.get(index).getType();
            this.outSchema.add(type, col);
        }

    }

    @Override
    public Iterator<RowData> iterator() {
        // 迭代器实现（仅处理简单投影）
        class ProjectIterator implements Iterator<RowData> {
            private final Iterator<RowData> sourceIterator;

            public ProjectIterator() {
                this.sourceIterator = ProjectOperator.this.source.iterator();
            }

            @Override
            public boolean hasNext() {
                return sourceIterator.hasNext();
            }

            @Override
            public RowData next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                RowData rowData = sourceIterator.next();

                List<Value> values = new ArrayList<>();
                for (int index : columnIndices) {
                    values.add(rowData.get(index));
                }
                return new RowData(values);
            }
        }
        return new ProjectIterator();
    }


    @Override
    public Schema computeSchema() {
        return this.outSchema;
    }

}
