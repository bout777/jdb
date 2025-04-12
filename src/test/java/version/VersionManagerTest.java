package version;

import com.jdb.common.Value;
import com.jdb.index.ClusterIndexEntry;
import com.jdb.index.Index;
import com.jdb.index.IndexEntry;
import com.jdb.index.IndexMetaData;
import com.jdb.table.Record;
import com.jdb.table.Table;
import com.jdb.transaction.TransactionManager;
import index.MockTable;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;

public class VersionManagerTest {

Table table;
    @Before
    public void init() {
        table = MockTable.getTable();
    }

    @Test
    public void testMultiThreadSimpleInsert() {
        DeterministicRunner runner = new DeterministicRunner(2);
        Index bptree = table.getClusterIndex();
        Map<Integer, IndexEntry> expected = new ConcurrentHashMap<>();
        runner.run(1, () -> {
            TransactionManager.getInstance().begin();
            for (int i = 100; i < 200; i++) {
                Record record = MockTable.generateRecord(i);
                IndexEntry e = new ClusterIndexEntry(Value.ofInt(i), record);
                expected.put(i,e);
                bptree.insert(e);
            }
        });
        runner.run(0, () -> {
            TransactionManager.getInstance().begin();
            for (int i = 0; i < 100; i++) {
                Record record = MockTable.generateRecord(i);
                IndexEntry e = new ClusterIndexEntry(Value.ofInt(i), record);
                expected.put(i,e);
                bptree.insert(e);
            }
        });

        runner.joinAll();
        TransactionManager.getInstance().begin();
        for (int i = 0; i < 200; i++) {
            IndexEntry indexEntry = bptree.searchEqual(Value.ofInt(i));
            assertEquals(expected.get(i), indexEntry);
        }
    }

}
