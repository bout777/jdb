package com.jdb.index;

import com.jdb.catalog.Column;
import com.jdb.catalog.Schema;

public class IndexMetaData {
    public String tableName;
    public Column keyColumn;
    public String indexName;
    public Schema tableSchema;
    public int fid;
    public String getTableName() {
        return tableName;
    }

    public Schema getTableSchema() {
        return tableSchema;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Column getKeyColumn() {
        return keyColumn;
    }

    public void setKeyColumn(Column keyColumn) {
        this.keyColumn = keyColumn;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public IndexMetaData(String tableName, Column keyColumn, String indexName, Schema tableSchema, int fid) {
        this.tableName = tableName;
        this.keyColumn = keyColumn;
        this.indexName = indexName;
        this.tableSchema = tableSchema;
        this.fid = fid;
    }
}
