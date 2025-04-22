package com.jdb.engine;

import com.jdb.Engine;
import com.jdb.TestUtil;
import com.jdb.catalog.Schema;
import com.jdb.table.RowData;
import com.jdb.table.Table;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class EngineRecoveryTest {
    public static final String TEST_PATH = "testbaserec";
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
    public void testSimpleRollback(){
        var expected = new ArrayList<RowData>();
        expected.add(TestUtil.generateRecord(1));
        expected.add(TestUtil.generateRecord(2));
        expected.add(TestUtil.generateRecord(3));
        engine.beginTransaction();
        Schema schema = TestUtil.recordSchema();
        engine.createTable("student",schema);
        Table student_table = engine.getTableManager().getTable("student");
        engine.insert("student",expected.get(0));
        engine.insert("student",expected.get(1));
        engine.insert("student",expected.get(2));
        engine.commit();

        engine.beginTransaction();
        engine.delete("student",expected.get(0).getPrimaryKey());
        engine.delete("student",expected.get(1).getPrimaryKey());

        var iter = student_table.scan();
        assertEquals(iter.next(),expected.get(2));
        assertFalse(iter.hasNext());
        engine.abort();

        iter = student_table.scan();
        var actual = new ArrayList<RowData>();
        while (iter.hasNext()){
            actual.add(iter.next());
        }
        assertEquals(expected,actual);
    }

    @Test
    public void testRecoveryRestart(){
        var expected = new ArrayList<RowData>();
        for (int i = 0; i < 2000; i++) {
            expected.add(TestUtil.generateRecord(i));
        }
    }





}
