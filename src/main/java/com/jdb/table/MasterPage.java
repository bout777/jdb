package com.jdb.table;

import com.jdb.recovery.RecoveryManager;
import com.jdb.storage.Page;
import com.jdb.transaction.TransactionContext;

import java.nio.ByteBuffer;

public class MasterPage {
    private Page page;
    public static final int ROOT_OFFSET = 0;
    private ByteBuffer buffer;
    private RecoveryManager recoveryManager;

    public MasterPage(Page page,RecoveryManager rm){
        this.page = page;
        this.recoveryManager = rm;
        buffer = page.getBuffer();
    }

    public long getRootPageId(){
        return buffer.getLong(ROOT_OFFSET);
    }

    public void setRootPageId(long rootPageId){
        long xid = TransactionContext.getTransaction().getXid();
        recoveryManager.logMasterPageUpdate(xid, page.pid, getRootPageId(), rootPageId);
        buffer.putLong(ROOT_OFFSET, rootPageId);
    }

    public long getPageId() {
        return page.pid;
    }
}
