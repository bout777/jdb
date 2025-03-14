import com.idme.storage.BufferPool;
import com.idme.storage.Disk;
import com.idme.table.Record;
import com.idme.table.Table;
import org.junit.Before;
import org.junit.Test;

public class TableTest {
    Table table;
    BufferPool bufferPool;
    Disk disk;
    @Before
    public  void init()
    {
         disk = new Disk();
         bufferPool = new BufferPool(disk);
        table = new Table(bufferPool);
    }
//    @Test
//    public void testInert()
//    {
//        System.out.println("insert");
//        for (int i = 0; i < 10; i++) {
//        table.insertRecord(generateRecord());
//
//        }
//        bufferPool.flush();
//    }

    @Test
    public void testRead()
    {
        for (int i = 0; i < 10; i++) {
            Record r= table.getRecord(0,i);
            System.out.println(r);
        }
    }

    public Record generateRecord()
    {
        Record record = new Record();
        record.primaryKey = 1;
        record.value = new int[]{1,2,3,4,5,6,7,8,9,10};
        record.isDeleted = 0;
        record.size = record.value.length*Integer.BYTES+Integer.BYTES*2+Byte.BYTES;
        return record;
    }
}
