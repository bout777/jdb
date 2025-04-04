package com.jdb.common;

public class Constants {
    public final static int PAGE_SIZE = 16 * 1024;

    //offset,size,primary key
    public final static int SLOT_SIZE = 3 * Integer.BYTES;

    public final static int NULL_PAGE_ID = -1;

    public final static int NULL_LSN = 0;

    public static final String LOG_FILE_PATH = "log";
}
