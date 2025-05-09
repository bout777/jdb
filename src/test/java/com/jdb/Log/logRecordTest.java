package com.jdb.Log;

import com.jdb.recovery.logs.*;
import com.jdb.storage.BufferPool;
import com.jdb.table.DataPage;
import com.jdb.table.PagePointer;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class logRecordTest {
    DataPage dataPage;
    BufferPool bufferPool;

    @Before
    public void init() {
//        bufferPool = BufferPool.getInstance();
//        com.jdb.Table table = TestUtil.getTable();
//        dataPage = new DataPage(bufferPool.newPage(LOG_FILE_ID),bufferPool, rm);
    }

    @Test
    public void SimpleSerialize() {
        MasterLog expected = new MasterLog(10L);
        var buf = ByteBuffer.allocate(200);
        expected.serialize(buf, 0);

        LogRecord log = LogRecord.deserialize(buf, 0);
        assertEquals(expected, log);

        var image = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        InsertLog insertLog = new InsertLog(114514L, 0, new PagePointer(0, 0), image);
        insertLog.serialize(buf, 0);

        log = LogRecord.deserialize(buf, 0);
        assertEquals(insertLog, log);
    }

    @Test
    public void testUpdateLog() {
        UpdateLog expected = new UpdateLog(114514L, 0, 333L, (short) 0, new byte[]{1, 2, 3, 3}, new byte[]{4, 5, 6, 6});
        ByteBuffer buffer = ByteBuffer.allocate(100);
        expected.serialize(buffer, 0);
        LogRecord log = LogRecord.deserialize(buffer, 0);
        assertEquals(expected, log);
    }

    @Test
    public void testAllocPageLog() {
        var buf = ByteBuffer.allocate(200);
        AllocPageLog expected = new AllocPageLog(114514L, 0, 0);
        expected.serialize(buf, 0);
        LogRecord log = LogRecord.deserialize(buf, 0);
        assertEquals(expected, log);
    }

}
