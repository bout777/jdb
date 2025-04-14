package version;

import com.jdb.common.Value;
import com.jdb.index.ClusterIndexEntry;
import com.jdb.index.Index;
import com.jdb.index.IndexEntry;
import com.jdb.table.RowData;
import com.jdb.table.Table;
import com.jdb.transaction.TransactionManager;
import com.jdb.version.ReadResult;
import com.jdb.version.VersionManager;
import index.MockTable;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
                    RowData rowData = MockTable.generateRecord(j);
                    IndexEntry e = new ClusterIndexEntry(Value.ofInt(j), rowData);
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
        /*
        * 事务1修改记录a
        * 事务2查询记录a，不可见
        * 事务1提交
        * 事务2查询记录a，可见*/
        DeterministicRunner runner = new DeterministicRunner(2);
        var vm = VersionManager.getInstance();
        RowData rowData = MockTable.generateRecord(20);
        var tm = TransactionManager.getInstance();
        runner.run(0, () -> {
            tm.begin();
            vm.pushUpdate(table.getTableName(), rowData);
        });
        runner.run(1, () -> {
            tm.begin();
            var key = Value.ofInt(rowData.getPrimaryKey());
            var result = vm.read(table.getTableName(),key );
            assertEquals(ReadResult.Visibility.INVISIBLE, result.getVisibility());
        });
        runner.run(0,()-> {
            TransactionManager.getInstance().commit();
        });
        runner.run(1,()->{
            var key = Value.ofInt(rowData.getPrimaryKey());
            var result = vm.read(table.getTableName(), key);
            assertEquals(ReadResult.Visibility.VISIBLE, result.getVisibility());
            assertEquals(rowData, result.getRowData());
        });
    }

}
