package com.jdb.common;

public class Constants {
    public final static int PAGE_SIZE = 16 * 1024;

    //offset,size,primary key
    public final static int SLOT_SIZE = 3 * Integer.BYTES;

    public final static long NULL_PAGE_ID = -1L;

    public final static int NULL_LSN = 0;

    public static final int LOG_FILE_ID = 369;
}
