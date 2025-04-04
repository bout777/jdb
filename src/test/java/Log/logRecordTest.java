package Log;

import Table.TableTest;
import com.jdb.catalog.ColumnList;
import com.jdb.common.Value;
import com.jdb.recovery.LogRecord;
import com.jdb.recovery.logs.MasterLog;
import com.jdb.recovery.logs.UpdateLog;
import com.jdb.storage.BufferPool;
import com.jdb.table.DataPage;
import com.jdb.table.Record;
import com.jdb.table.RecordID;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class logRecordTest {
    DataPage dataPage;

    @Before
    public void init() {
        dataPage = new DataPage(0, BufferPool.getInstance().getPage(0));

    }

    @Test
    public void SimpleSerialize() {
        MasterLog expected = new MasterLog(10L);
        byte[] bytes = new byte[10];
        expected.serialize(ByteBuffer.wrap(bytes), 0);

        LogRecord log = LogRecord.deserialize(ByteBuffer.wrap(bytes), 0);
        assertEquals(expected, log);
    }

    @Test
    public void testUpdateLog() {
        UpdateLog expected = new UpdateLog(114514L,0,333L,(short) 0,new byte[]{1,2,3,3},new byte[]{4,5,6,6});
        ByteBuffer buffer = ByteBuffer.allocate(100);
        expected.serialize(buffer, 0);
        LogRecord log = LogRecord.deserialize(buffer, 0);
        assertEquals(expected, log);
    }

    @Test
    public void f() {
        Record record = dataPage.getRecord(10, ColumnList.instance);
        record.values.set(0, Value.ofString("xixi"));
        record.values.set(1, Value.ofInt(100));
        dataPage.insertRecord(10, record);
        System.out.println(dataPage.getRecord(10, ColumnList.instance));
    }
}
