package com.jdb.storage;

public interface EvictionStrategy {
    void hit(long pid);
    
}
