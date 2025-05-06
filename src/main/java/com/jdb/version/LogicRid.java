package com.jdb.version;

import com.jdb.common.value.Value;

import java.util.Objects;

public class LogicRid {
    private final String tableName;
    private final Value<?> primaryKey;
    LogicRid(String tableName, Value<?> primaryKey) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof LogicRid that)
            return this.tableName.equals(that.tableName) && this.primaryKey == that.primaryKey;
        return false;
    }
    @Override
    public int hashCode() {
        return Objects.hash(tableName, primaryKey);
    }
}
