//package com.jdb.Log;
//
//import com.jdb.recovery.LogManager;
//import com.jdb.recovery.logs.LogRecord;
//import com.jdb.recovery.LogType;
//import com.jdb.recovery.RecoveryManager;
//import com.jdb.recovery.logs.DeleteLog;
//import com.jdb.recovery.logs.InsertLog;
//import com.jdb.recovery.logs.UpdateLog;
//import com.jdb.storage.BufferPool;
//import com.jdb.table.PagePointer;
//import com.jdb.table.Table;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import static org.junit.Assert.assertEquals;
//
//public class LogManagerTest {
//
////    private BufferPool bufferPool = BufferPool.getInstance();
////    Table table = Table.getTestTable();
////    private LogManager logManager = RecoveryManager.getInstance().getLogManager();
//    @Before
//    public void setUp() {
////         table = TestUtil.getTable();
////       var dataPage = new DataPage(BufferPool.getInstance().newPage(LOG_FILE_ID));
////        logManager = RecoveryManager.getInstance().getLogManager();
//    }
//
////    @Test
////    public void testSimpleAppendAndGet() {
////        LogRecord expected = new MasterLog(114514L);
////        long lsn = logManager.append(expected);
////        LogRecord logRecord = logManager.getLogRecord(lsn);
////        assertEquals(expected, logRecord);
////    }
//
//    @Test
//    public void testIterScan() {
//        List<LogRecord> expected = new ArrayList<>();
//
//        for (int i = 0; i < 100; i++) {
//            expected.add(genatateLogRecord(LogType.UPDATE));
//        }
//        for (LogRecord l : expected) {
//            logManager.append(l);
//        }
//
//        var iter = logManager.scanFrom(expected.get(0).getLsn());
//
//        for (int i = 0; i < 100; i++) {
//            assertEquals(expected.get(i), iter.next());
//        }
//    }
//    @Test
//    public void testIterInsert(){
//        var image = new byte[]{1,2,3,4,5,6,7,8,9,10};
//        List<LogRecord> expected = new ArrayList<>();
//
//        for (int i = 0; i < 1000; i++) {
//            InsertLog insertLog = new InsertLog(114514L, 0, new PagePointer(0, 0),image);
//            expected.add(insertLog);
//        }
//
//        for (LogRecord l : expected) {
//            logManager.append(l);
//        }
//
//        var iter = logManager.scan();
//        for(LogRecord l:expected){
//            assertEquals(l, iter.next());
//        }
//    }
//
//    @Test
//    public void testIterDelete(){
//        var image = new byte[]{1,2,3,4,5,6,7,8,9,10};
//        List<LogRecord> expected = new ArrayList<>();
//
//        for (int i = 0; i < 1000; i++) {
//            DeleteLog deleteLog = new DeleteLog(114514L, 0, new PagePointer(0, 0),image);
//            expected.add(deleteLog);
//        }
//
//        for (LogRecord l : expected) {
//            logManager.append(l);
//        }
//
//        var iter = logManager.scan();
//        for(LogRecord l:expected){
//            assertEquals(l, iter.next());
//        }
//    }
//
////    @Test
////    public void testInsertLogRedo() throws InterruptedException {
////        Page page = bufferPool.newPage(table.getTableName());
////        DataPage dataPage = new DataPage(page,bufferPool);
////        dataPage.init();
////
////        List<RowData> expected = new ArrayList<>();
////        RecoveryManager.getInstance().setLogManager(logManager);
////        TransactionContext.setTransactionContext(new TransactionContext(2L));
////        RecoveryManager.getInstance().registerTransaction(2L);
////
////        for (int i = 0; i < 10; i++) {
////            RowData rowData = generateRecord(i);
////            expected.add(rowData);
////            dataPage.insertRecord(rowData, true, true);
////        }
////        dataPage.init();
////
////        var logIter = logManager.scan();
////        while (logIter.hasNext()) {
////            logIter.next().redo(bufferPool);
////            System.out.println("31");
////        }
////
////
////        var iter = dataPage.scanFrom(0);
////        for (RowData r : expected) {
////            assertEquals(r, iter.next());
////        }
////    }
//
//    @Test
//    public void testScanFrom() {
//
//    }
//
//    private int getLSNPage(long lsn) {
//        return (int) lsn >> Integer.SIZE;
//    }
//
//    private int getLSNOffset(long lsn) {
//        return (int) lsn & Integer.MAX_VALUE;
//    }
//
//    private LogRecord genatateLogRecord(LogType type) {
//        var before = new byte[]{1, 1, 1, 1};
//        var after = new byte[]{7, 7, 7, 7};
//        LogRecord log = new UpdateLog(11L, 0, 0L, (short) 0, before, after);
//        return log;
//    }
//
//}
