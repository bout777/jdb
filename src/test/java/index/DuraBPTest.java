package index;

import Table.TableTest;
import com.jdb.common.Value;
import com.jdb.index.ClusterIndexEntry;
import com.jdb.index.Index;
import com.jdb.index.IndexEntry;
import com.jdb.table.RowData;
import com.jdb.table.Table;
import com.jdb.transaction.TransactionManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DuraBPTest {
    Random r = new Random();


    Index bpTree;
    TableTest tt;

    @Before
    public void init() {
        Table table = MockTable.getTable();
        bpTree = table.getClusterIndex();
        TransactionManager.getInstance().begin();
    }

    @After
    public void clean() {

    }

    @Test
    public void testDescInsertAndSearch() {
        List<IndexEntry> expect = new ArrayList<>();
        for (int i = 2000; i >= 0; i--) {
            RowData rowData = MockTable.generateRecord(i);
            IndexEntry e = new ClusterIndexEntry(Value.ofInt(i), rowData);
            bpTree.insert(e, true);
            expect.add(e);
        }
        for (int i = 0; i <= 2000; i++) {
            IndexEntry e = bpTree.searchEqual(Value.ofInt(i));
            assertEquals(Value.ofInt(i), e.getKey());
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
            RowData rowData = MockTable.generateRecord(id);
            bpTree.insert(new ClusterIndexEntry(Value.ofInt(id), rowData), true);
        }

        for (Integer id : ids) {
            IndexEntry e = bpTree.searchEqual(Value.ofInt(id));
            assertEquals(Value.ofInt(id), e.getKey());
        }
    }

    @Test
    public void testSimpleDelete() {
        var row = MockTable.generateRecord(114);
        bpTree.insert(new ClusterIndexEntry(Value.ofInt(114), row), true);
        IndexEntry entry = bpTree.searchEqual(Value.ofInt(114));
        assertEquals(Value.ofInt(114), entry.getKey());

        bpTree.delete(Value.ofInt(114), true);
        try {
            IndexEntry res = bpTree.searchEqual(Value.ofInt(114));
            fail("并非删除: " + res);
        } catch (NoSuchElementException e) {

        }
    }

    @Test
    public void testInnerNodeDelete() {
        for (int i = 2000; i >= 0; i--) {
            RowData rowData = MockTable.generateRecord(i);
            IndexEntry e = new ClusterIndexEntry(Value.ofInt(i), rowData);
            bpTree.insert(e, true);
        }

        bpTree.delete(Value.ofInt(1743), true);
        bpTree.delete(Value.ofInt(199), true);
        try {
            IndexEntry res = bpTree.searchEqual(Value.ofInt(1743));
            fail("并非删除: " + res);
        } catch (NoSuchElementException e) {
        }
        try {
            IndexEntry res = bpTree.searchEqual(Value.ofInt(199));
            fail("并非删除: " + res);
        } catch (NoSuchElementException e) {
        }
    }


}
