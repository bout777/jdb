package com.jdb.query;

import com.jdb.Engine;
import com.jdb.TestUtil;
import com.jdb.common.DataType;
import com.jdb.common.PredicateOperator;
import com.jdb.common.value.Value;
import com.jdb.table.RowData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class BasicQueryTest {
    String dir = TestUtil.TEST_DIR;
    File testDir = new File(dir);
    Engine engine;

    @Before
    public void init() {
        TestUtil.cleanfile(dir);
        engine = new Engine(dir);
        engine.beginTransaction();
        engine.createTable("student", TestUtil.recordSchema());
        engine.commit();
    }

    @After
    public void close() throws IOException {
        engine.close();
        TestUtil.cleanfile(dir);
    }

    @Test
    public void testScanAll() {
        engine.beginTransaction();

        for (int i = 0; i < 10; i++) {
            engine.insert("student", TestUtil.generateRecord(i));
        }

        engine.commit();


        var op = new SeqScanOperator("student", engine);
        var iter = op.iterator();
        while (iter.hasNext()) {
            var row = iter.next();
            System.out.println(row);
        }
    }

    @Test
    public void testSelect() {
        var expected = new ArrayList<RowData>();
        for (int i = 0; i < 10; i++) {
            expected.add(TestUtil.generateRecord(i));
        }
        engine.beginTransaction();
        for (int i = 0; i < 10; i++) {
            engine.insert("student", expected.get(i));
        }
        engine.commit();

        var op = new SeqScanOperator("student", engine);
        op.setSchema(TestUtil.recordSchema());
        var select = new SelectOperator(op, "id", PredicateOperator.LESS_THAN, Value.of(5));
        var iter = select.iterator();

        for (int i = 0; i < 5; i++) {
            assertEquals(expected.get(i), iter.next());
        }
        assertFalse(iter.hasNext());
        select = new SelectOperator(op, "id", PredicateOperator.EQUALS, Value.of(5));
        iter = select.iterator();
        assertEquals(iter.next(), expected.get(5));
        assertFalse(iter.hasNext());
    }

    @Test
    public void testProject() {
        var expected = new ArrayList<RowData>();
        for (int i = 0; i < 10; i++) {
            expected.add(TestUtil.generateRecord(i));
        }
        engine.beginTransaction();
        for (int i = 0; i < 10; i++) {
            engine.insert("student", expected.get(i));
        }
        engine.commit();

        var op = new SeqScanOperator("student", engine);
        op.setSchema(TestUtil.recordSchema());


        var cols = new ArrayList<String>();
        cols.add("name");
        cols.add("age");
        var project = new ProjectOperator(op, cols);

        var iter = project.iterator();
        while (iter.hasNext()) {
            var row = iter.next();
            assertEquals(row.get(0), Value.of("hehe"));
            assertEquals(row.get(1).getType(), DataType.INTEGER);
        }
    }


}
