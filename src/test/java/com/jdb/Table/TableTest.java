//package com.jdb.Table;
//
//import com.jdb.TestUtil;
//import com.jdb.common.value.Value;
//import com.jdb.table.RowData;
//import com.jdb.table.Table;
//import com.jdb.transaction.TransactionManager;
//import org.junit.Before;
//import org.junit.Test;
//
//import static org.junit.Assert.*;
//
//public class TableTest {
//    Table table;
//
//    @Before
//    public void init() {
//        table = TestUtil.getTable();
//        TransactionManager.getInstance().begin();
//    }
//
//    public void testInsert() {
//
//    }
//
//    public void testDelete() {
//
//    }
//
//    @Test
//    public void testUpdate() {
//        var rowData = TestUtil.generateRecord(147);
//        table.insertRecord(rowData, true,true);
//        var before = table.getRowData(Value.of(147));
//
//        assertEquals(rowData, before);
//
//        rowData = TestUtil.generateRecord(147);
//        table.updateRecord(Value.of(147), rowData, true);
//        RowData after = table.getRowData(Value.of(147));
//
//        assertEquals(rowData, after);
//    }
//}
