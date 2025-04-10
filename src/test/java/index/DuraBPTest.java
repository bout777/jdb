package index;

import Table.TableTest;
import com.jdb.catalog.Schema;
import com.jdb.common.Value;
import com.jdb.index.BPTree;
import com.jdb.index.ClusterIndexEntry;
import com.jdb.index.IndexEntry;
import com.jdb.index.IndexMetaData;
import com.jdb.recovery.LogManager;
import com.jdb.recovery.RecoveryManager;
import com.jdb.storage.BufferPool;
import com.jdb.table.Record;
import com.jdb.table.Table;
import com.jdb.transaction.TransactionContext;
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
        Table table = new MockTable().getTable();
        Schema schema = table.getSchema();
        IndexMetaData metaData = new IndexMetaData(table.getTableName(),schema.columns().get(0),"test",schema);
        bpTree = new BPTree(metaData);
        RecoveryManager.getInstance().setLogManager(new LogManager());
//        TransactionContext.setTransactionContext(new TransactionContext(2L));
//        RecoveryManager.getInstance().startTransaction(2L);

    }

    @Test
    public void testSimpleInsertAndSearch() {
        List<IndexEntry> expect = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            Record record = MockTable.generateRecord(i);
            IndexEntry e = new ClusterIndexEntry(Value.ofInt(i), record);
            bpTree.insert(e);
            expect.add(e);
        }
//        BufferPool.getInstance().flush();
//        BufferPool.getInstance().shutdown();
        for (int i = 0; i < 2000; i++) {
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
