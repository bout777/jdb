import com.idme.catalog.ColumnDef;
import com.idme.catalog.ColumnList;
import com.idme.common.DataType;
import com.idme.common.Value;
import com.idme.storage.BufferPool;
import com.idme.storage.Disk;
import com.idme.table.PagePointer;
import com.idme.table.Record;
import com.idme.table.Table;
import com.idme.table.TableScanner;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class TableTest {
    Table table;
    BufferPool bufferPool;
    Disk disk;
    ColumnList columnList;
    Random r = new Random();

    @Before
    public void init() {
//        disk = new Disk();
        bufferPool = BufferPool.getInstance();
        columnList = new ColumnList();
        columnList.add(new ColumnDef(DataType.STRING, "name"));
        columnList.add(new ColumnDef(DataType.INTEGER, "age"));

        table = new Table(bufferPool, columnList);
    }

    @Test
    public void testInert() {
        System.out.println("insert");
        for (int i = 0; i < 10000; i++)
            table.insertRecord(generateRecord(i));
        bufferPool.flush();
    }


    public  Record generateRecord(int i) {
        Record record = new Record();
        record.primaryKey = i;
        record.isDeleted = 0;

        record.size += Integer.BYTES * 2 + Byte.BYTES;

        record.values.add(Value.ofString("hehe"));
        record.values.add(Value.ofInt(r.nextInt()));

        for (Value val : record.values) {
            record.size += val.getBytes();
        }

        return record;
    }

    public Value generateValue() {
        return Value.ofInt(1);
    }

    @Test
    public void ScannerTest() {
        testInert();
        TableScanner sc = new TableScanner(bufferPool, table);
        PagePointer p = new PagePointer(0, 0);

        for (int i = 0; i < 10000; i++) {
            Record r = sc.getNextRecord(p);
            System.out.println(i + ":" + r);
        }
    }
}
