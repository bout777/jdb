
import com.idme.catalog.ColumnDef;
import com.idme.catalog.ColumnList;
import com.idme.common.DataType;
import com.idme.common.Value;
import com.idme.storage.BufferPool;
import com.idme.storage.Disk;
import com.idme.table.Record;
import com.idme.table.Table;

import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class TableTest {
    Table table;
    BufferPool bufferPool;
    Disk disk;
    ColumnList columnList;
    Random  r = new Random();
    @Before
    public void init() {
        disk = new Disk();
        bufferPool = new BufferPool(disk);
        columnList = new ColumnList();
        columnList.add(new ColumnDef(DataType.STRING, "name"));
        columnList.add(new ColumnDef(DataType.INTEGER, "age"));
        table = new Table(bufferPool,columnList);
    }
    @Test
    public void testInert()
    {
        System.out.println("insert");
        for (int i = 0; i < 10000; i++)
            table.insertRecord(generateRecord());

        bufferPool.flush();
    }

    @Test
    public void testRead() {
        for (int i = 0; i < 10000; i++) {
            Record r = table.getRecord(0, i);
            System.out.println(r);
        }
    }

    public Record generateRecord() {
        Record record = new Record();
        record.primaryKey = 1;
        record.isDeleted = 0;

        record.size += Integer.BYTES * 2 + Byte.BYTES;

        record.values.add(Value.ofString("hehe"));
        record.values.add(Value.ofInt(r.nextInt()));

        for(Value val: record.values){
            record.size+=val.getBytes();
        }

        return record;
    }

    public Value generateValue() {
        return Value.ofInt(1);
    }
}
