package index;

import com.jdb.catalog.ColumnDef;
import com.jdb.catalog.Schema;
import com.jdb.common.DataType;
import com.jdb.common.Value;
import com.jdb.recovery.LogManager;
import com.jdb.recovery.RecoveryManager;
import com.jdb.storage.BufferPool;
import com.jdb.table.Record;
import com.jdb.table.Table;
import com.jdb.transaction.TransactionContext;
import com.jdb.transaction.TransactionManager;

import java.util.Random;

public class MockTable {
    private static Table table;
    private static Random r = new Random();
    static {
        BufferPool bufferPool = BufferPool.getInstance();

        bufferPool.newFile(777,"test.db");
        bufferPool.newFile(369,"log");
        Schema schema = new Schema();
        schema.add(new ColumnDef(DataType.STRING, "name"));
        schema.add(new ColumnDef(DataType.INTEGER, "age"));
        table = new Table("test.db", schema);

        RecoveryManager.getInstance().setLogManager(new LogManager());
        TransactionManager.getInstance().begin();
    }
    public static Table getTable() {
        return table;
    }
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

}
