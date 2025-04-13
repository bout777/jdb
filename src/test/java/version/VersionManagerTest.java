package version;

import com.jdb.common.Value;
import com.jdb.index.ClusterIndexEntry;
import com.jdb.index.Index;
import com.jdb.index.IndexEntry;
import com.jdb.table.Record;
import com.jdb.table.Table;
import com.jdb.transaction.TransactionManager;
import index.MockTable;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
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
    public void testMultiThreadSimpleInsert(){
//        DeterministicRunner runner = new DeterministicRunner(4);
        Index bptree = table.getClusterIndex();
        Map<Integer, IndexEntry> expected = new ConcurrentHashMap<>();
//        runner.run(1, () -> {
//            TransactionManager.getInstance().begin();
//            for (int i = 0; i < 500; i++) {
//                Record record = MockTable.generateRecord(i);
//                IndexEntry e = new ClusterIndexEntry(Value.ofInt(i), record);
//                expected.put(i,e);
//                bptree.insert(e);
//            }
//        });
//        runner.run(0, () -> {
//            TransactionManager.getInstance().begin();
//            for (int i = 500; i < 1000; i++) {
//                Record record = MockTable.generateRecord(i);
//                IndexEntry e = new ClusterIndexEntry(Value.ofInt(i), record);
//                expected.put(i,e);
//                bptree.insert(e);
//            }
//        });
//        runner.run(2,()->{
//            TransactionManager.getInstance().begin();
//            for (int i = 1500; i >= 1000; i--) {
//                Record record = MockTable.generateRecord(i);
//                IndexEntry e = new ClusterIndexEntry(Value.ofInt(i), record);
//                expected.put(i,e);
//                bptree.insert(e);
//            }
//        });
//
//        runner.joinAll();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            int finalI = i;
            threads.add(new Thread(() -> {
                TransactionManager.getInstance().begin();
                for (int j = finalI * 10; j < (finalI + 1) * 10; j++) {
                    Record record = MockTable.generateRecord(j);
                    IndexEntry e = new ClusterIndexEntry(Value.ofInt(j), record);
                    expected.put(j, e);
                    bptree.insert(e);
                }
            }));
        }
        threads.forEach(Thread::start);

        threads.forEach(t->{
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        TransactionManager.getInstance().begin();
        for (int i = 0; i < 100; i++) {
            IndexEntry indexEntry = bptree.searchEqual(Value.ofInt(i));
            assertEquals(expected.get(i), indexEntry);
        }
    }

    @Test
    public void testVisibility() {

    }

}
