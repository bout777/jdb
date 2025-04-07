package index;

import com.jdb.catalog.ColumnDef;
import com.jdb.catalog.Schema;
import com.jdb.common.DataType;
import com.jdb.storage.BufferPool;
import com.jdb.table.Table;

public class MockTable {
    private Table table;

    public MockTable() {
        BufferPool bufferPool = BufferPool.getInstance();
        Schema schema = new Schema();
        schema.add(new ColumnDef(DataType.STRING, "name"));
        schema.add(new ColumnDef(DataType.INTEGER, "age"));
        table = new Table("test", schema);
    }
    public Table getTable() {
        return table;
    }
}
