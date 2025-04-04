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

import java.util.Random;

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
    public void testInsert() {
        for (int i = 0; i < 1000; i++) {
            Record record = tt.generateRecord(i);
            IndexEntry e = new ClusterIndexEntry(Value.ofInt(i), record);
            bpTree.insert(e);
        }
        for (int i = 0; i < 1000; i++) {
            IndexEntry e = bpTree.searchEqual(Value.ofInt(r.nextInt(1000)));
            System.out.println(e);
        }
//
//        for (int i = 0; i < 100; i++){
//            IndexEntry e = bpTree.searchEqual(Value.ofInt(30));
//            System.out.println(e);
//        }
        BufferPool.getInstance().flush();
    }

    @Test
    public void testSearch() {
        for (int i = 0; i < 1000; i++) {
            IndexEntry e = bpTree.searchEqual(Value.ofInt(r.nextInt(1000)));
            System.out.println(e);
        }
    }
}
