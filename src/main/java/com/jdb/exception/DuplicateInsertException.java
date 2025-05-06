package com.jdb.exception;

public class DuplicateInsertException extends DatabaseException {
    public DuplicateInsertException(String message) {
        super(message);
    }
}
