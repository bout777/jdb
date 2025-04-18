package com.jdb.index;

import com.jdb.catalog.Column;
import com.jdb.catalog.Schema;

public class IndexMetaData {
    public String tableName;
    public Column keySchema;
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

    public Column getKeySchema() {
        return keySchema;
    }

    public void setKeySchema(Column keySchema) {
        this.keySchema = keySchema;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public IndexMetaData(String tableName, Column keySchema, String indexName, Schema tableSchema, int fid) {
        this.tableName = tableName;
        this.keySchema = keySchema;
        this.indexName = indexName;
        this.tableSchema = tableSchema;
        this.fid = -1;
    }
}
