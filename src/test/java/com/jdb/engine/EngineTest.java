package com.jdb.engine;

import com.jdb.Engine;
import com.jdb.TestUtil;
import com.jdb.catalog.Column;
import com.jdb.catalog.Schema;
import com.jdb.common.DataType;
import com.jdb.common.Value;
import com.jdb.table.RowData;
import com.jdb.table.Table;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class EngineTest {
    public static final String TEST_PATH = "testbase";
    private Engine engine;
    private String fileName;
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void init() throws Exception {
        File testDir = folder.newFolder(TEST_PATH);
        fileName = testDir.getAbsolutePath();
        engine = new Engine(fileName);
    }

    @Test
    public void testInit() {

    }

    @Test
    public void testCreateTable() {
        Schema schema = TestUtil.recordSchema();
        engine.createTable("student.table",schema);
    }

    @Test
    public void testSimpleInsert() {
        testCreateTable();
        var rowData = TestUtil.generateRecord(3);
        engine.beginTransaction();
        engine.insert("student.table",rowData);

        var tbm = engine.getTableManager();
        Table table = tbm.getTable("student.table");

        var row = table.getRowData(rowData.getPrimaryKey());
        assertEquals(rowData,row);

        row =(RowData) table.getClusterIndex().searchEqual(rowData.getPrimaryKey()).getValue();
        assertEquals(rowData,row);
        System.out.println(row);
    }
}
