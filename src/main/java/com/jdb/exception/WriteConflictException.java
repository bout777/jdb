package com.jdb.exception;

public class WriteConflictException extends RuntimeException {
    public WriteConflictException(String message) {
        super(message);
    }
}
