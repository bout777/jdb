import com.jdb.Engine;
import com.jdb.catalog.Column;
import com.jdb.catalog.Schema;
import com.jdb.common.DataType;
import com.jdb.common.Value;
import com.jdb.table.RowData;
import com.jdb.table.Table;
import com.jdb.table.TableManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class EngineTest {
    public static final String TEST_PATH = "testbase";
    private Engine engine;
    private String fileName;
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void init() throws Exception {
        File testDir = folder.newFolder(TEST_PATH);
        fileName = testDir.getAbsolutePath();
        engine = new Engine(fileName);
    }

    @Test
    public void testInit() {

    }

    @Test
    public void testCreateTable() {
        Schema schema = new Schema()
                .add(new Column(DataType.STRING, "name"))
                .add(new Column(DataType.INTEGER, "age"));

        engine.createTable("student.table",schema);
    }

    @Test
    public void testInsert() {
        testCreateTable();
        var rowData = TestUtil.generateRecord(3);
        engine.beginTransaction();
        engine.insert("student.table",rowData);

        var tbm = engine.getTableManager();
        Table table = tbm.getTable("student.table");

        var row = table.getRowData(Value.ofInt(rowData.getPrimaryKey()));
        assertEquals(rowData,row);

    }
}
