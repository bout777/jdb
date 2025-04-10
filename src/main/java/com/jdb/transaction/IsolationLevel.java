package com.jdb.transaction;

public enum IsolationLevel {
    SNAPSHOT_ISOLATION,
    READ_COMMITTED;
    private static final IsolationLevel[] values = IsolationLevel.values();

    public static IsolationLevel fromInt(int i) {
        return values[i - 1];
    }

    public int getValue() {
        return this.ordinal() + 1;
    }
}
