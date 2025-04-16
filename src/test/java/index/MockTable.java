package index;

import com.jdb.catalog.Column;
import com.jdb.catalog.Schema;
import com.jdb.common.DataType;
import com.jdb.common.Value;
import com.jdb.recovery.LogManager;
import com.jdb.recovery.RecoveryManager;
import com.jdb.storage.BufferPool;
import com.jdb.table.RowData;
import com.jdb.table.Table;

import java.util.Random;

public class MockTable {
    private static Table table;
    private static Random r = new Random();
    static {
        BufferPool bufferPool = BufferPool.getInstance();

        bufferPool.newFile(777,"test.db");
        bufferPool.newFile(369,"log");
        Schema schema = new Schema();
        schema.add(new Column(DataType.STRING, "name"));
        schema.add(new Column(DataType.INTEGER, "age"));
        table = new Table("test.db", schema);

        RecoveryManager.getInstance().setLogManager(new LogManager());
    }
    public static Table getTable() {
        return table;
    }
    public static RowData generateRecord(int i) {
        RowData rowData = new RowData();

        rowData.primaryKey = i;
        rowData.isDeleted = 0;

        rowData.size += Integer.BYTES * 2 + Byte.BYTES;

        rowData.values.add(Value.ofString("hehe"));
        rowData.values.add(Value.ofInt(r.nextInt()));

        for (Value val : rowData.values) {
            rowData.size += val.getBytes();
        }

        return rowData;
    }

}
