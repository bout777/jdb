package Log;

import com.jdb.common.Value;
import com.jdb.recovery.LogManager;
import com.jdb.recovery.LogRecord;
import com.jdb.recovery.LogType;
import com.jdb.recovery.RecoveryManager;
import com.jdb.recovery.logs.InsertLog;
import com.jdb.recovery.logs.MasterLog;
import com.jdb.recovery.logs.UpdateLog;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Page;
import com.jdb.table.DataPage;
import com.jdb.table.Record;
import com.jdb.table.RecordID;
import com.jdb.table.Table;
import com.jdb.transaction.TransactionContext;
import index.MockTable;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.jdb.common.Constants.LOG_FILE_ID;
import static org.junit.Assert.assertEquals;

public class LogManagerTest {

    private BufferPool bufferPool = BufferPool.getInstance();
    private LogManager logManager = new LogManager();
    Table table;
    @Before
    public void setUp() {
         table = MockTable.getTable();
       var dataPage = new DataPage(BufferPool.getInstance().newPage(LOG_FILE_ID));
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

        for (int i = 0; i < 4; i++) {
            expected.add(genatateLogRecord(LogType.UPDATE));
        }
        for (LogRecord l : expected) {
            logManager.append(l);
        }

        var iter = logManager.scanFrom(expected.get(0).getLsn());

        for (int i = 0; i < 4; i++) {
            assertEquals(expected.get(i), iter.next());
        }
    }
    @Test
    public void testIterInsert(){
        var image = new byte[]{1,2,3,4,5,6,7,8,9,10};
        List<LogRecord> expected = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            InsertLog insertLog = new InsertLog(114514L, 0, new RecordID(0, 0),image);
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
    public void testScanFrom() {

    }

    @Test
    public void testInsertLogRedo() throws InterruptedException {
        Page page = bufferPool.newPage(table.getTableName());
        DataPage dataPage = new DataPage(page);
        dataPage.init();

        List<Record> expected = new ArrayList<>();
            RecoveryManager.getInstance().setLogManager(logManager);
            TransactionContext.setTransactionContext(new TransactionContext(2L));
            RecoveryManager.getInstance().startTransaction(2L);

            for (int i = 0; i < 10; i++) {
                Record record = generateRecord(i);
                expected.add(record);
                dataPage.insertRecord(record, true, true);
            }
            dataPage.init();

        var logIter = logManager.scan();
        while (logIter.hasNext()) {
            logIter.next().redo();
        }


        var iter = dataPage.scanFrom(0);
        for (Record r : expected) {
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

    public Record generateRecord(int i) {
        Record record = new Record();

        record.primaryKey = i;
        record.isDeleted = 0;

        record.size += Integer.BYTES * 2 + Byte.BYTES;

        record.values.add(Value.ofString("hehe"));
        record.values.add(Value.ofInt(1414810));

        for (Value val : record.values) {
            record.size += val.getBytes();
        }

        return record;
    }
}
