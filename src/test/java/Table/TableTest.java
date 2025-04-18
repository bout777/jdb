package Table;

import com.jdb.common.Value;
import com.jdb.table.RowData;
import com.jdb.table.Table;
import com.jdb.transaction.TransactionManager;
import index.MockTable;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TableTest {
    Table table;

    @Before
    public void init() {
        table = MockTable.getTable();
        TransactionManager.getInstance().begin();
    }

    public void testInsert() {

    }

    public void testDelete() {

    }

    @Test
    public void testUpdate() {
        var rowData = MockTable.generateRecord(147);
        table.insertRecord(rowData, true,true);
        var before = table.getRowData(Value.ofInt(147));

        assertEquals(rowData, before);

        rowData = MockTable.generateRecord(147);
        table.updateRecord(Value.ofInt(147), rowData, true);
        RowData after = table.getRowData(Value.ofInt(147));

        assertEquals(rowData, after);
    }
}
