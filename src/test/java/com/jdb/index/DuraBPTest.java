package com.jdb.index;

import com.jdb.DummyBufferPool;
import com.jdb.DummyRecoverManager;
import com.jdb.TestUtil;
import com.jdb.common.Value;
import com.jdb.recovery.logs.LogRecord;
import com.jdb.table.RowData;
import com.jdb.table.Table;
import com.jdb.transaction.TransactionContext;
import com.jdb.version.DeterministicRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class DuraBPTest {
    Random r = new Random();


    BPTree bpTree;


    @Before
    public void init() {
        TransactionContext.setTransactionContext(new TransactionContext(1));
        var mata = new IndexMetaData("table1",TestUtil.recordSchema().get(0), "index1", TestUtil.recordSchema(), 0);
        var bp = new DummyBufferPool();
        bpTree = new BPTree(mata,bp, new DummyRecoverManager());
        bpTree.init();
    }

    @After
    public void clean() {
        TransactionContext.unsetTransaction();
    }

    @Test
    public void testDescInsertAndSearch() {
        List<RowData> expect = new ArrayList<>();
        for (int i = 2000; i >= 0; i--) {
            var rowData = TestUtil.generateRecord(i);
            var e = new ClusterIndexEntry(Value.of(i), rowData);
            bpTree.insert(e, true);
            expect.add(e.rowData);
        }
        for (int i = 0; i <= 2000; i++) {
            IndexEntry e = bpTree.searchEqual(Value.of(i));
            assertEquals(Value.of(i),((RowData)e.getValue()).getPrimaryKey());
        }

    }

    @Test
    public void testRandomInsert() {
        List<Integer> ids = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            ids.add(i);
        }
        Collections.shuffle(ids);
        for (Integer id : ids) {
            RowData rowData = TestUtil.generateRecord(id);
            bpTree.insert(new ClusterIndexEntry(Value.of(id), rowData), true);
        }

        for (Integer id : ids) {
            IndexEntry e = bpTree.searchEqual(Value.of(id));
            assertEquals(Value.of(id), e.getKey());
        }
    }

    @Test
    public void testSimpleDelete() {
        var row = TestUtil.generateRecord(114);
        bpTree.insert(new ClusterIndexEntry(Value.of(114), row), true);
        IndexEntry entry = bpTree.searchEqual(Value.of(114));
        assertEquals(Value.of(114), entry.getKey());

        bpTree.delete(Value.of(114), true);
        try {
            IndexEntry res = bpTree.searchEqual(Value.of(114));
            fail("并非删除: " + res);
        } catch (NoSuchElementException e) {

        }
    }

    @Test
    public void testInnerNodeDelete() {
        for (int i = 2000; i >= 0; i--) {
            RowData rowData = TestUtil.generateRecord(i);
            IndexEntry e = new ClusterIndexEntry(Value.of(i), rowData);
            bpTree.insert(e, true);
        }

        bpTree.delete(Value.of(1743), true);
        bpTree.delete(Value.of(199), true);
        try {
            IndexEntry res = bpTree.searchEqual(Value.of(1743));
            fail("并非删除: " + res);
        } catch (NoSuchElementException e) {
        }
        try {
            IndexEntry res = bpTree.searchEqual(Value.of(199));
            fail("并非删除: " + res);
        } catch (NoSuchElementException e) {
        }
    }

    @Test
    public void testDelete() {
        var expected = new ArrayList<RowData>();
        for (int i = 0; i < 2000; i++) {
            expected.add(TestUtil.generateRecord(i));
        }
        var runner = new DeterministicRunner(2);
        runner.run(0, () -> {
            TransactionContext.setTransactionContext(new TransactionContext(2));
            for (int i = 1000; i < 2000; i++) {
                bpTree.insert(new ClusterIndexEntry(Value.of(i), expected.get(i)), true);
            }
        });
        runner.run(1, () -> {
            TransactionContext.setTransactionContext(new TransactionContext(3));
            for (int i = 0; i < 1000; i++) {
                bpTree.insert(new ClusterIndexEntry(Value.of(i), expected.get(i)), true);
            }
        });

        for (int i = 0; i < 2000; i++) {
            assertEquals(expected.get(i), bpTree.searchEqual(Value.of(i)).getValue());
        }

    }

    @Test
    public void testBulkDelete() {
        for (int i = 1000; i <2000 ; i++) {
            RowData rowData = TestUtil.generateRecord(i);
            IndexEntry e = new ClusterIndexEntry(Value.of(i), rowData);
            bpTree.insert(e, true);
        }

        for (int i = 0;i<1000; i++) {
            RowData rowData = TestUtil.generateRecord(i);
            IndexEntry e = new ClusterIndexEntry(Value.of(i), rowData);
            bpTree.insert(e, true);
        }
        for (int i = 1999;i>=1000; i--) {
           bpTree.delete(Value.of(i), true);
        }

    }

    @Test
    public void testScanAll(){
        for (int i = 2000; i >= 0 ; i--) {
            RowData rowData = TestUtil.generateRecord(i);
            IndexEntry e = new ClusterIndexEntry(Value.of(i), rowData);
            bpTree.insert(e, true);
        }

        var iterator = bpTree.scanAll();

        for (int i = 0; i <= 2000; i++) {
            RowData next = iterator.next();
            assertEquals(Value.of(i), next.getPrimaryKey());
        }

        assertFalse(iterator.hasNext());
    }




}
