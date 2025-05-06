package com.jdb.exception;

public class WriteConflictException extends DatabaseException {
    public WriteConflictException(String message) {
        super(message);
    }
}
