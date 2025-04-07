package Table;

import com.jdb.catalog.ColumnDef;
import com.jdb.catalog.Schema;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class TableTest {
    static Random r = new Random();
    Table table;
    BufferPool bufferPool;
    Disk disk;
    Schema schema;

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
        schema = new Schema();
        schema.add(new ColumnDef(DataType.STRING, "name"));
        schema.add(new ColumnDef(DataType.INTEGER, "age"));

        table = new Table("test", schema);
    }

    @Test
    public void testInert() {
        System.out.println("insert");
        List<Record> expected = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            Record re = generateRecord(i);
            expected.add(re);
            table.insertRecord(re);
        }
        bufferPool.flush();
        bufferPool.shutdown();

        TableScanner sc = new TableScanner(bufferPool, table);
        RecordID p = new RecordID(0, 0);

        for (int i = 0; i < 10000; i++) {
            Record r = sc.getNextRecord(p);
            assertEquals(r, expected.get(i));
        }
    }

    public Value generateValue() {
        return Value.ofInt(1);
    }

    @Test
    public void ScannerTest() {
//        testInert();

    }
}
