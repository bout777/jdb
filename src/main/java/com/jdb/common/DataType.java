package com.jdb.common;

public enum DataType {
    NULL,       // 0
    INTEGER,    // 1
    STRING,     // 2
    BOOLEAN,    // 3
    FLOAT,
    BYTE_ARRAY;// 4
    public static DataType fromString(String s) {
        return switch (s.toUpperCase()) {
            case "NULL"       -> NULL;
            case "INTEGER"    -> INTEGER;
            case "STRING"     -> STRING;
            case "BOOLEAN"    -> BOOLEAN;
            case "FLOAT"      -> FLOAT;
            case "BYTE_ARRAY" -> BYTE_ARRAY;
            default           -> null;
        };
    }
}