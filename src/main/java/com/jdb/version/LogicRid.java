package com.jdb.version;

import java.util.Objects;

public class LogicRid {
    private final String tableName;
    private final int primaryKey;
    LogicRid(String tableName, int primaryKey) {
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
