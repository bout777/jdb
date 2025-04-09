package com.jdb.version;

import com.jdb.table.Record;

public class VersionEntry {
    //the trx that insert or update on this record
    public long xid;
    //content
    public Record record;
}
