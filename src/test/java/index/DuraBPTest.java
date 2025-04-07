package index;

import Table.TableTest;
import com.jdb.common.Value;
import com.jdb.index.BPTree;
import com.jdb.index.ClusterIndexEntry;
import com.jdb.index.IndexEntry;
import com.jdb.storage.BufferPool;
import com.jdb.table.Record;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class DuraBPTest {
    Random r = new Random();


    BPTree bpTree;
    TableTest tt;

    @Before
    public void init() {
        BufferPool bp = BufferPool.getInstance();
        bpTree = new BPTree(bp);
        tt = new TableTest();
    }

    @Test
    public void testSimpleInsertAndSearch() {
        List<IndexEntry> expect = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            Record record = tt.generateRecord(i);
            IndexEntry e = new ClusterIndexEntry(Value.ofInt(i), record);
            bpTree.insert(e);
            expect.add(e);
        }
        BufferPool.getInstance().flush();
        BufferPool.getInstance().shutdown();
        for (int i = 0; i < 5000; i++) {
            IndexEntry e = bpTree.searchEqual(Value.ofInt(i));
            assertEquals(expect.get(i), e);
        }

    }

    @Test
    public void testSearch() {
        for (int i = 0; i < 1000; i++) {
            IndexEntry e = bpTree.searchEqual(Value.ofInt(r.nextInt(1000)));
        }
    }
}
