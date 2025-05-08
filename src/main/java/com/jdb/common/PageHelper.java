package com.jdb.common;

public class PageHelper {
    public static long concatPid(int fid, int pno) {
        return (((long) fid) << 32) | pno;
    }

    public static int getPno(long pid) {
        return (int) (pid & 0xffffffffL);
    }

    public static int getFid(long pid) {
        return (int) (pid >> 32);
    }
}
