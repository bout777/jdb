package com.jdb.engine;

import com.jdb.Engine;
import com.jdb.TestUtil;
import com.jdb.catalog.Schema;
import com.jdb.recovery.LogType;
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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;

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
        var p = Path.of(TestUtil.TEST_DIR);
        cleanfile();
        fileName = testDir.getAbsolutePath();
        engine = new Engine(fileName);
    }

    @After
    public void close() throws IOException {
        engine.close();
        cleanfile();
    }

    void cleanfile() {
        try {
            var p = Path.of(TestUtil.TEST_DIR);
            Files.walk(p)
                    .filter(path -> !path.equals(p))
                    .sorted(Comparator.reverseOrder())
                    .forEach(file -> {
                        try {
                            Files.delete(file);
                        } catch (IOException e) {
                            throw new RuntimeException("删除失败: " + file, e);
                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        Table table = engine.createTable("student", schema);
        engine.commit();

        var runner = new DeterministicRunner(2);
        runner.run(0, () -> {
            Table student_table = engine.getTableManager().getTable("student");
            engine.beginTransaction();
            for (int i = 1000; i < 2000; i++) {
                engine.insert("student", expected.get(i));
            }
        });
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
    public void testSimpleCrashRecovery() {


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
        var row  =TestUtil.generateRecord(1);
        table.insertRecord(row, true, true);
        engine.commit();
        engine.close();

        // 模拟程序崩溃
        engine = new Engine(fileName);

        Table student_tb = engine.getTableManager().getTable("student");
        var iter = student_tb.scan();
        assertEquals(iter.next(), row);
    }

    @Test
    public void testCrashRecWithSpilt() {
        var expected = new ArrayList<RowData>();
        for (int i = 0; i < 1000; i++) {
            expected.add(TestUtil.generateRecord(i));
        }
        engine.beginTransaction();
        Schema schema = TestUtil.recordSchema();
        engine.createTable("student", schema);
        engine.commit();
        Table table = engine.getTableManager().getTable("student");//
        assert table != null;

        var logiter = engine.getLogManager().scan();
        engine.beginTransaction();
        for (var row : expected) {
//            engine.insert("student",row);
            table.insertRecord(row, true, true);
        }
        engine.commit();
//        while (logiter.hasNext()) {
//            LogRecord log = logiter.next();
//            if(log.getType() != LogType.INSERT)
//            System.out.println(log);
//        }
        engine.close();

        // 模拟程序崩溃
        engine = new Engine(fileName);
        Table student_tb = engine.getTableManager().getTable("student");
        student_tb.getClusterIndex();
        var iter = student_tb.scan();
//        while (iter.hasNext()) System.out.println(iter.next());
        for (var row : expected) {
            assertEquals(row, iter.next());
        }
    }


}
