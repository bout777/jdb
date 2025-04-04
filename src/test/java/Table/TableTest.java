package Table;

import com.jdb.catalog.ColumnDef;
import com.jdb.catalog.ColumnList;
import com.jdb.common.DataType;
import com.jdb.common.Value;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Disk;
import com.jdb.table.Record;
import com.jdb.table.RecordID;
import com.jdb.table.Table;
import com.jdb.table.TableScanner;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class TableTest {
    static Random r = new Random();
    Table table;
    BufferPool bufferPool;
    Disk disk;
    ColumnList columnList;

    public static Record generateRecord(int i) {
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

    public Value generateValue() {
        return Value.ofInt(1);
    }

    @Test
    public void ScannerTest() {
        testInert();
        TableScanner sc = new TableScanner(bufferPool, table);
        RecordID p = new RecordID(0, 0);

        for (int i = 0; i < 10000; i++) {
            Record r = sc.getNextRecord(p);
            System.out.println(i + ":" + r);
        }
    }
}
