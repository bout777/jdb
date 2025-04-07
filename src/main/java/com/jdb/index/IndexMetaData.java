package com.jdb.index;

import com.jdb.catalog.ColumnDef;
import com.jdb.catalog.Schema;

public class IndexMetaData {
    private String tableName;
    private ColumnDef keySchema;
    private String indexName;
    private Schema tableSchema;
    public String getTableName() {
        return tableName;
    }

    public Schema getTableSchema() {
        return tableSchema;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public ColumnDef getKeySchema() {
        return keySchema;
    }

    public void setKeySchema(ColumnDef keySchema) {
        this.keySchema = keySchema;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public IndexMetaData(String tableName, ColumnDef keySchema, String indexName, Schema tableSchema) {
        this.tableName = tableName;
        this.keySchema = keySchema;
        this.indexName = indexName;
        this.tableSchema = tableSchema;
    }
}
