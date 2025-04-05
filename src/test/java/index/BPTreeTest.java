package index;

import com.jdb.common.Value;
import com.jdb.index.BPTree;
import com.jdb.index.ClusterIndexEntry;
import com.jdb.index.IndexEntry;
import com.jdb.storage.BufferPool;
import com.jdb.table.RecordID;
import org.junit.Before;
import org.junit.Test;

public class BPTreeTest {
    BPTree bpTree;

//    @Before
//    public void init() {
//        bpTree = new BPTree(BufferPool.getInstance());
//    }
//
//    @Test
//    public void insertTest() {
//        for (int i = 0; i < 1000; i++) {
//            bpTree.insert(generateEntry(i));
//        }
//        System.out.println(bpTree);
//
//    }
//
//    @Test
//    public void searchTest() {
//        insertTest();
//        for (int i = 0; i < 1000; i++) {
//            IndexEntry indexEntry = bpTree.searchEqual(Value.ofInt(i));
//            System.out.println(indexEntry.getKey());
//        }
//    }
//
//    @Test
//    public void loadfromPage() {
//
//    }
//
//    IndexEntry generateEntry(int i) {
//        return new ClusterIndexEntry(Value.ofInt(i), new RecordID(1, 1));
//    }
}
