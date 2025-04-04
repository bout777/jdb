//import com.idme.common.Value;
//import com.idme.index.BPTree;
//import com.idme.index.IndexEntry;
//import com.idme.table.PagePointer;
//import org.junit.Before;
//import org.junit.Test;
//
//public class BPTreeTest {
//    BPTree bpTree;
//
//    @Before
//    public void init() {
//        bpTree = new BPTree();
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
//        return new IndexEntry(Value.ofInt(i), new PagePointer(1, 1));
//    }
//}
