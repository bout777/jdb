package com.jdb.lock;

public class DummyLockContext extends LockContext {
    public DummyLockContext(String name) {
        this.name = new ResourceName(name);
    }
}
