package Log;

import com.jdb.common.Value;
import com.jdb.recovery.LogManager;
import com.jdb.recovery.logs.LogRecord;
import com.jdb.recovery.LogType;
import com.jdb.recovery.RecoveryManager;
import com.jdb.recovery.logs.DeleteLog;
import com.jdb.recovery.logs.InsertLog;
import com.jdb.recovery.logs.MasterLog;
import com.jdb.recovery.logs.UpdateLog;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Page;
import com.jdb.table.DataPage;
import com.jdb.table.RowData;
import com.jdb.table.PagePointer;
import com.jdb.table.Table;
import com.jdb.transaction.TransactionContext;
import index.MockTable;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class LogManagerTest {

    private BufferPool bufferPool = BufferPool.getInstance();
    Table table;
    private LogManager logManager ;
    @Before
    public void setUp() {
         table = MockTable.getTable();
//       var dataPage = new DataPage(BufferPool.getInstance().newPage(LOG_FILE_ID));
        logManager = RecoveryManager.getInstance().getLogManager();
    }

    @Test
    public void testSimpleAppendAndGet() {
        LogRecord expected = new MasterLog(114514L);
        long lsn = logManager.append(expected);
        LogRecord logRecord = logManager.getLogRecord(lsn);
        assertEquals(expected, logRecord);
    }

    @Test
    public void testIterScan() {
        List<LogRecord> expected = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            expected.add(genatateLogRecord(LogType.UPDATE));
        }
        for (LogRecord l : expected) {
            logManager.append(l);
        }

        var iter = logManager.scanFrom(expected.get(0).getLsn());

        for (int i = 0; i < 100; i++) {
            assertEquals(expected.get(i), iter.next());
        }
    }
    @Test
    public void testIterInsert(){
        var image = new byte[]{1,2,3,4,5,6,7,8,9,10};
        List<LogRecord> expected = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            InsertLog insertLog = new InsertLog(114514L, 0, new PagePointer(0, 0),image);
            expected.add(insertLog);
        }

        for (LogRecord l : expected) {
            logManager.append(l);
        }

        var iter = logManager.scan();
        for(LogRecord l:expected){
            assertEquals(l, iter.next());
        }
    }

    @Test
    public void testIterDelete(){
        var image = new byte[]{1,2,3,4,5,6,7,8,9,10};
        List<LogRecord> expected = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            DeleteLog deleteLog = new DeleteLog(114514L, 0, new PagePointer(0, 0),image);
            expected.add(deleteLog);
        }

        for (LogRecord l : expected) {
            logManager.append(l);
        }

        var iter = logManager.scan();
        for(LogRecord l:expected){
            assertEquals(l, iter.next());
        }
    }

    @Test
    public void testScanFrom() {

    }

    @Test
    public void testInsertLogRedo() throws InterruptedException {
        Page page = bufferPool.newPage(table.getTableName());
        DataPage dataPage = new DataPage(page);
        dataPage.init();

        List<RowData> expected = new ArrayList<>();
            RecoveryManager.getInstance().setLogManager(logManager);
            TransactionContext.setTransactionContext(new TransactionContext(2L));
            RecoveryManager.getInstance().registerTransaction(2L);

            for (int i = 0; i < 10; i++) {
                RowData rowData = generateRecord(i);
                expected.add(rowData);
                dataPage.insertRecord(rowData, true, true);
            }
            dataPage.init();

        var logIter = logManager.scan();
        while (logIter.hasNext()) {
            logIter.next().redo();
        }


        var iter = dataPage.scanFrom(0);
        for (RowData r : expected) {
            assertEquals(r, iter.next());
        }
    }

    private int getLSNPage(long lsn) {
        return (int) lsn >> Integer.SIZE;
    }

    private int getLSNOffset(long lsn) {
        return (int) lsn & Integer.MAX_VALUE;
    }

    private LogRecord genatateLogRecord(LogType type) {
        var before = new byte[]{1, 1, 1, 1};
        var after = new byte[]{7, 7, 7, 7};
        LogRecord log = new UpdateLog(11L, 0, 0L, (short) 0, before, after);
        return log;
    }

    public RowData generateRecord(int i) {
        RowData rowData = new RowData();

        rowData.primaryKey = i;
        rowData.isDeleted = 0;

        rowData.size += Integer.BYTES * 2 + Byte.BYTES;

        rowData.values.add(Value.ofString("hehe"));
        rowData.values.add(Value.ofInt(1414810));

        for (Value val : rowData.values) {
            rowData.size += val.getBytes();
        }

        return rowData;
    }
}
