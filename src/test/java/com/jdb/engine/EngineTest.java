package com.jdb.engine;

import com.jdb.Engine;
import com.jdb.TestUtil;
import com.jdb.catalog.Schema;
import com.jdb.common.value.Value;
import com.jdb.table.RowData;
import com.jdb.table.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class EngineTest {
    public static final String TEST_PATH = "testbase";
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private Engine engine;
    private String fileName;

    @Before
    public void init() throws Exception {
        File testDir = folder.newFolder(TEST_PATH);
        fileName = testDir.getAbsolutePath();
        engine = new Engine(fileName);
        engine.beginTransaction();
    }

    @After
    public void cleanup() {
        engine.commit();
    }


    @Test
    public void testInit() {

    }

    @Test
    public void testCreateTable() {
        Schema schema = TestUtil.recordSchema();
        engine.createTable("student", schema);
    }

    @Test
    public void testSimpleInsert() {
        testCreateTable();
        var rowData = TestUtil.generateRecord(3);
//        engine.beginTransaction();
        engine.insert("student", rowData);

        var tbm = engine.getTableManager();
        Table table = tbm.getTable("student");

        var row = table.getRowData(rowData.getPrimaryKey());
        assertEquals(rowData, row);

        row = (RowData) table.getClusterIndex().searchEqual(rowData.getPrimaryKey()).getValue();
        assertEquals(rowData, row);
        //      System.out.println(row);
    }

    @Test
    public void testInsertAndSearch() {
        testCreateTable();
//        engine.beginTransaction();
        List<RowData> expected = new ArrayList<>();
        List<RowData> actual = new ArrayList<>();
        for (int i = 1; i <= 1000; i++) {
            var rowData = TestUtil.generateRecord(i);
            expected.add(rowData);
            engine.insert("student", rowData);
        }

        Table table = engine.getTableManager().getTable("student");

        var index = table.getClusterIndex();

        for (int i = 1; i <= 1000; i++) {
            var row = (RowData) index.searchEqual(Value.of(i)).getValue();
            actual.add(row);
        }
        assertEquals(expected, actual);
    }


}
