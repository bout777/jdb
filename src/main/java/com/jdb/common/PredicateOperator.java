package com.jdb.common;

public enum PredicateOperator {
    EQUALS,
    NOT_EQUALS,
    LESS_THAN,
    LESS_THAN_EQUALS,
    GREATER_THAN,
    GREATER_THAN_EQUALS;

    public <T extends Comparable<T>> boolean evaluate(T a, T b) {
        return switch (this) {
            case EQUALS -> a.compareTo(b) == 0;
            case NOT_EQUALS -> a.compareTo(b) != 0;
            case LESS_THAN -> a.compareTo(b) < 0;
            case LESS_THAN_EQUALS -> a.compareTo(b) <= 0;
            case GREATER_THAN -> a.compareTo(b) > 0;
            case GREATER_THAN_EQUALS -> a.compareTo(b) >= 0;
        };
    }

    public String toSymbol() {
        return switch (this) {
            case EQUALS -> "=";
            case NOT_EQUALS -> "!=";
            case LESS_THAN -> "<";
            case LESS_THAN_EQUALS -> "<=";
            case GREATER_THAN -> ">";
            case GREATER_THAN_EQUALS -> ">=";
        };
    }

    public PredicateOperator reverse() {
        return switch (this) {
            case LESS_THAN -> GREATER_THAN;
            case LESS_THAN_EQUALS -> GREATER_THAN_EQUALS;
            case GREATER_THAN -> LESS_THAN;
            case GREATER_THAN_EQUALS -> LESS_THAN_EQUALS;
            default -> this;
        };
    }
}
