package com.jdb.common;

public class Utils {
    public static long concatPid(int fid,int pno){
        return (((long)fid)<<32)|pno;
    }
}
