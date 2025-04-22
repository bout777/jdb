package com.jdb.common;

public class Constants {
    public final static int PAGE_SIZE = 16 * 1024;

    //offset,size,primary key
    public final static int SLOT_SIZE = 2 * Integer.BYTES;

    public final static long NULL_PAGE_ID = -1L;

    public final static int NULL_LSN = 0;

    public final static long NULL_XID = 0;

    public static final int LOG_FILE_ID = 369;

    public static final String LOG_FILE_NAME = "log";

    public static final String TABLE_FILE_SUFFIX = ".table";

    public static final int TABLE_META_DATA_FILE_ID = 114;

    public static final String TABLE_META_DATA_FILE_NAME = "table_meta";

    public static final int INDEX_META_DATA_FILE_ID = 514;

    public static final String INDEX_META_DATA_FILE_NAME = "index_meta";

    public static final int FILE_META_DATA_FILE_ID = 0;

    public static final String FILE_META_DATA_FILE_NAME = "file_meta";


}
