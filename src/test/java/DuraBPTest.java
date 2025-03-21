import com.idme.common.Value;
import com.idme.index.BPTree;
import com.idme.index.ClusterIndexEntry;
import com.idme.index.IndexEntry;
import com.idme.storage.BufferPool;
import com.idme.table.Record;
import org.junit.Before;
import org.junit.Test;

public class DuraBPTest {

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
            IndexEntry e = bpTree.searchEqual(Value.ofInt(i));
            System.out.println(e);
        }
    }
}
