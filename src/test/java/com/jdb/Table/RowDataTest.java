package com.jdb.Table;

import com.jdb.TestUtil;
import com.jdb.common.Value;
import com.jdb.table.RowData;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class RowDataTest {
    @Test
    public void testSimpleSerialize() {
        var buf = ByteBuffer.allocate(100);
        RowData rowData1 = TestUtil.generateRecord(13);
        RowData rowData2 = TestUtil.generateRecord(14);
        rowData1.serializeTo(buf, 0);
        rowData2.serializeTo(buf, rowData1.getSize());
        RowData rowData3 = RowData.deserialize(buf, 0,TestUtil.recordSchema());
        RowData rowData4 = RowData.deserialize(buf, rowData3.getSize(),TestUtil.recordSchema());
        assertEquals(rowData3, rowData1);
        assertEquals(rowData2, rowData4);
    }
}
