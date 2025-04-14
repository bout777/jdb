package index;

import Table.TableTest;
import com.jdb.common.Value;
import com.jdb.index.*;
import com.jdb.table.RowData;
import com.jdb.table.Table;
import com.jdb.transaction.TransactionManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class DuraBPTest {
    Random r = new Random();


    Index bpTree;
    TableTest tt;

    @Before
    public void init() {
        Table table = new MockTable().getTable();
        bpTree = table.getClusterIndex();
    }

    @After
    public void clean() {

    }

    @Test
    public void testDescInsertAndSearch() {
        List<IndexEntry> expect = new ArrayList<>();
        TransactionManager.getInstance().begin();
        for (int i = 2000; i >=0 ; i--) {
            RowData rowData = MockTable.generateRecord(i);
            IndexEntry e = new ClusterIndexEntry(Value.ofInt(i), rowData);
            bpTree.insert(e);
            expect.add(e);
        }
//        BufferPool.getInstance().flush();
//        BufferPool.getInstance().shutdown();
        for (int i = 0; i <= 2000; i++) {
            IndexEntry e = bpTree.searchEqual(Value.ofInt(i));
            assertEquals(Value.ofInt(i), e.getKey());
        }

    }

    @Test
    public void testRandomInsert() {
        TransactionManager.getInstance().begin();
        List<Integer> ids = new ArrayList<>();
        for(int i = 0; i < 2000; i++){
            ids.add(i);
        }
        Collections.shuffle(ids);
        for(Integer id: ids){
            RowData rowData = MockTable.generateRecord(id);
            bpTree.insert(new ClusterIndexEntry(Value.ofInt(id), rowData));
        }

        for(Integer id: ids){
            IndexEntry e = bpTree.searchEqual(Value.ofInt(id));
            assertEquals(Value.ofInt(id), e.getKey());
        }
    }

}
