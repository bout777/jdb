package Log;

import com.jdb.common.Value;
import com.jdb.recovery.LogManager;
import com.jdb.recovery.LogRecord;
import com.jdb.recovery.LogType;
import com.jdb.recovery.logs.MasterLog;
import com.jdb.recovery.logs.UpdateLog;
import com.jdb.storage.BufferPool;
import com.jdb.table.Record;
import com.jdb.table.RecordID;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class LogManagerTest {

    private BufferPool bufferPool = BufferPool.getInstance();
    private LogManager logManager = new LogManager();

    @Before
    public void setUp() {

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
            assertEquals(expected.get(i),iter.next());
        }
    }

    @Test
    public void testScanFrom() {

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
