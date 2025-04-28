package com.jdb.engine;

import com.jdb.Engine;
import com.jdb.TestUtil;
import com.jdb.catalog.Schema;
import com.jdb.recovery.logs.LogRecord;
import com.jdb.table.RowData;
import com.jdb.table.Table;
import com.jdb.version.DeterministicRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class EngineRecoveryTest {
    public static final String TEST_PATH = "testbaserec";
    private Engine engine;
    private String fileName;
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    File testDir = new File(TestUtil.TEST_DIR);

    @Before
    public void init() throws Exception {
//        File testDir = folder.newFolder(TEST_PATH);
        fileName = testDir.getAbsolutePath();
        engine = new Engine(fileName);
    }

    @After
    public void close() {
        engine.close();
        testDir.deleteOnExit();
    }

    @Test
    public void testSimpleRollback() {
        var expected = new ArrayList<RowData>();
        expected.add(TestUtil.generateRecord(1));
        expected.add(TestUtil.generateRecord(2));
        expected.add(TestUtil.generateRecord(3));
        engine.beginTransaction();
        Schema schema = TestUtil.recordSchema();
        engine.createTable("student", schema);
        Table student_table = engine.getTableManager().getTable("student");
        engine.insert("student", expected.get(0));
        engine.insert("student", expected.get(1));
        engine.insert("student", expected.get(2));
        engine.commit();

        engine.beginTransaction();
        engine.delete("student", expected.get(0).getPrimaryKey());
        engine.delete("student", expected.get(1).getPrimaryKey());

        var iter = student_table.scan();
        assertEquals(iter.next(), expected.get(2));
        assertFalse(iter.hasNext());
        engine.abort();

        iter = student_table.scan();
        var actual = new ArrayList<RowData>();
        while (iter.hasNext()) {
            actual.add(iter.next());
        }
        assertEquals(expected, actual);
    }

    @Test
    public void testRollbackWithSpilt() {
        var expected = new ArrayList<RowData>();
        for (int i = 0; i < 2000; i++) {
            expected.add(TestUtil.generateRecord(i));
        }
        engine.beginTransaction();
        Schema schema = TestUtil.recordSchema();
        engine.createTable("student", schema);
        engine.commit();

        var runner = new DeterministicRunner(2);
        runner.run(0, () -> {
            Table student_table = engine.getTableManager().getTable("student");
            engine.beginTransaction();
            for (int i = 1000; i < 2000; i++) {
                engine.insert("student", expected.get(i));
            }
        });
        Iterator<LogRecord> logiter = engine.getLogManager().scan();
        runner.run(1, () -> {
            Table student_table = engine.getTableManager().getTable("student");
            engine.beginTransaction();
            for (int i = 0; i < 1000; i++) {
                engine.insert("student", expected.get(i));
            }
        });

        runner.run(0, () -> {
            engine.abort();
        });

        runner.run(1, () -> {
            engine.commit();
        });
        Table student_table = engine.getTableManager().getTable("student");

        var iter = student_table.scan();
        for (int i = 0; i < 1000; i++) {
            assertEquals(expected.get(i), iter.next());
        }
        assertFalse(iter.hasNext());

        runner.joinAll();

    }

    @Test
    public void testCrashRecovery() {


        var expected = new ArrayList<RowData>();
//        for (int i = 0; i < 2000; i++) {
//            expected.add(TestUtil.generateRecord(i));
//        }
        engine.beginTransaction();
        Schema schema = TestUtil.recordSchema();
        engine.createTable("student", schema);
        engine.commit();
        Table table = engine.getTableManager().getTable("student");//
        assert table != null;

//        var logiter = engine.getLogManager().scan();
//        while (logiter.hasNext()) {
//            LogRecord log = logiter.next();
//            System.out.println(log);
//        }
        engine.beginTransaction();
        table.insertRecord(TestUtil.generateRecord(1), true, true);
        engine.commit();
        engine.close();

        // 模拟程序崩溃
        engine = new Engine(fileName);

        Table student_tb = engine.getTableManager().getTable("student");
        var iter = student_tb.scan();
        while (iter.hasNext()) System.out.println(iter.next());

    }


}
