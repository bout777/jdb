package com.jdb.recovery;

public enum LogType {
    INSERT,
    DELETE,
    UPDATE,

    COMMIT,
    ABORT,
    BEGIN,

    ALLOC_PAGE,
    FREE_PAGE,
    PAGE_LINK,

    DATA_PAGE_INIT,
    INDEX_PAGE_INIT,
    INDEX_PAGE_INSERT,
    MASTER_PAGE_UPDATE,

    CREATE_FILE,
    DELETE_FILE,

    COMPENSATION,
    END,
    MASTER,
    CHECKPOINT;

    private static LogType[] values = LogType.values();

    public static LogType fromInt(int t) {
        if (t <= 0 || t > values.length) {
            String err = String.format("Unknown TypeId ordinal %d.", t);
            throw new IllegalArgumentException(err);
        }
        return values[t-1];
    }

    public int getValue() {
        return ordinal()+1;
    }
}
