package com.jdb.version;

import com.jdb.table.Record;

public class VersionEntry {
    //write timestamp(the xid of the transaction that update/create this version)
    public long wts;
    //read timestamp(the max xid of all transactions that read this version)
    public long rts;
    //content
    public Record record;
}
