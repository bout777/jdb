package com.jdb.exception;

public class DuplicateInsertException extends RuntimeException {
    public DuplicateInsertException(String message) {
        super(message);
    }
}
