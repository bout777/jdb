package com.jdb;

public class TestCli {
    public static void main(String[] args) {
        TestUtil.cleanfile("demo");
        com.jdb.cli.CommandLine.main(args);
        TestUtil.cleanfile("demo");
    }
}
