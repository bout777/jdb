package com.jdb.recover;

import com.jdb.DummyBufferPool;
import com.jdb.recovery.LogManager;
import com.jdb.recovery.LogType;
import com.jdb.recovery.RecoveryManager;
import com.jdb.recovery.logs.BeginLog;
import com.jdb.recovery.logs.CommitLog;
import com.jdb.storage.BufferPool;
import com.jdb.table.PagePointer;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class RecoverTest {
    RecoveryManager rm;
    @Before
    public void init() {
        BufferPool bufferPool = new DummyBufferPool();
        rm = new RecoveryManager(bufferPool);
        rm.setLogManager(new LogManager(bufferPool));
    }
    @Test
    public void testSimpleLog(){
        rm.logTrxBegin(1L);
        rm.logInsert(1L, new PagePointer(1,1), new byte[]{1,2,3,4});
        rm.logInsert(1L, new PagePointer(2,1), new byte[]{1,2,3,4});
        rm.logCommit(1L);

        LogManager logManager = rm.getLogManager();
        var iter = logManager.scan();
        assertEquals(iter.next(), new BeginLog(1));
        assertEquals(iter.next().getType(), LogType.INSERT);
        assertEquals(iter.next().getType(), LogType.INSERT);
        assertEquals(iter.next(), new CommitLog(1));
        assertFalse(iter.hasNext());
    }

    @Test
    public void testCheckpoint() {
        rm.logTrxBegin(1L);
        rm.logInsert(1L, new PagePointer(1,1), new byte[]{1,2,3,4});
        rm.logInsert(1L, new PagePointer(2,1), new byte[]{1,2,3,4});

        rm.logTrxBegin(2L);
        rm.logInsert(2L, new PagePointer(3,1), new byte[]{1,2,3,4});
        rm.logInsert(2L, new PagePointer(4,1), new byte[]{1,2,3,4});

        rm.checkpoint();

        var iter = rm.getLogManager().scan();
        assertEquals(iter.next(), new BeginLog(1));
        assertEquals(iter.next().getType(), LogType.INSERT);
        assertEquals(iter.next().getType(), LogType.INSERT);

        assertEquals(iter.next(), new BeginLog(2));
        assertEquals(iter.next().getType(), LogType.INSERT);
        assertEquals(iter.next().getType(), LogType.INSERT);

        assertEquals(iter.next().getType(), LogType.CHECKPOINT);

        assertFalse(iter.hasNext());
    }

    @Test
    public void testSimpleRecover(){
        rm.logTrxBegin(1L);
        rm.logInsert(1L, new PagePointer(1,1), new byte[]{1,2,3,4});
        rm.logInsert(1L, new PagePointer(2,1), new byte[]{1,2,3,4});

        rm.logTrxBegin(2L);
        rm.logInsert(2L, new PagePointer(3,1), new byte[]{1,2,3,4});
        rm.logInsert(2L, new PagePointer(4,1), new byte[]{1,2,3,4});

        rm.logCommit(1L);
        rm.checkpoint();


    }



//    @Test
//
//    public void testSimpleTrx() {
//        var rowDate = TestUtil.generateRecord(123);
//        TransactionManager.getInstance().begin();
//        table.insertRecord(rowDate, true, true);
//        TransactionManager.getInstance().commit();
//        var iter = rm.getLogManager().scan();
//
//        assertEquals(iter.next(), new BeginLog(1));
//        assertEquals(iter.next().getType(), LogType.INSERT);
//        assertEquals(iter.next(), new CommitLog(1));
//        assertFalse(iter.hasNext());
//    }
//
//    @Test
//    public void testMultipleTrx() {
//        var runner = new DeterministicRunner(2);
//        List<LogRecord> expect = new ArrayList<>();
//        var tm = TransactionManager.getInstance();
//        runner.run(0, () -> {
//            tm.begin();
//            table.insertRecord(TestUtil.generateRecord(123), true, true);
//        });
//        runner.run(1, () -> {
//            tm.begin();
//            table.insertRecord(TestUtil.generateRecord(456), true, true);
//        });
//        runner.run(1, () -> {
//            tm.commit();
//        });
//        runner.run(0, () -> {
//            tm.commit();
//        });
//
//        var iter = rm.getLogManager().scan();
//        assertEquals(new BeginLog(1), iter.next());
//        assertEquals(LogType.INSERT, iter.next().getType());
//        assertEquals(new BeginLog(2), iter.next());
//        assertEquals(LogType.INSERT, iter.next().getType());
//        assertEquals(new CommitLog(2), iter.next());
//        assertEquals(new CommitLog(1), iter.next());
//        assertFalse(iter.hasNext());
//    }
//
//    @Test
//    public void testRecover() {
//        var rowbefore = TestUtil.generateRecord(123);
//        var rowafter = TestUtil.generateRecord(123);
//        var tm = TransactionManager.getInstance();
//        tm.begin();
//        table.insertRecord(rowbefore, true, true);
//        tm.commit();
//
//        tm.begin();
//        table.updateRecord(Value.ofInt(123), rowafter, true);
//        tm.commit();
//    }
//
//    @Test
//    public void testSimpleUndo() {
//        var tm = TransactionManager.getInstance();
//        var runner = new DeterministicRunner(1);
//        runner.run(0, () -> {
//            tm.begin();
//            table.insertRecord(TestUtil.generateRecord(123), true, true);
//            long xid = TransactionContext.getTransaction().getXid();
//            rm.rollback(xid);
//        });
//        tm.begin();
//        try{
//            table.getClusterIndex().searchEqual(Value.ofInt(123));
//            fail();
//        }catch (NoSuchElementException e){
//
//        }
//
//
//    }
//
//    @Test
//    public void testAbort() {
//        var tm = TransactionManager.getInstance();
//        var rowbefore = TestUtil.generateRecord(123);
//        var rowafter = TestUtil.generateRecord(123);
//        var rm = RecoveryManager.getInstance();
//        var runner = new DeterministicRunner(1);
//        runner.run(0, () -> {
//            tm.begin();
//            table.insertRecord(rowbefore, true, true);
//            table.updateRecord(Value.ofInt(123), rowafter, true);
//            long xid = TransactionContext.getTransaction().getXid();
//            rm.rollback(xid);
//        });
//        var lm = rm.getLogManager();
//        var iter = lm.scan();
//        assertEquals(new BeginLog(1), iter.next());
//        assertEquals(LogType.INSERT, iter.next().getType());
//        assertEquals(LogType.DELETE, iter.next().getType());
//        assertEquals(LogType.INSERT, iter.next().getType());
//        assertEquals(LogType.DELETE, iter.next().getType());
//        assertEquals(LogType.INSERT, iter.next().getType());
//        assertEquals(LogType.DELETE, iter.next().getType());
//        assertFalse(iter.hasNext());
//    }
//
//    @Test
//    public void testCheckpoint() {
//        var tm = TransactionManager.getInstance();
//        var runner = new DeterministicRunner(3);
//        runner.run(0, () -> {
//            tm.begin();
//            table.insertRecord(TestUtil.generateRecord(1), true, true);
//            table.insertRecord(TestUtil.generateRecord(2), true, true);
//            tm.commit();
//        });
//        runner.run(1, () -> {
//            tm.begin();
//            table.insertRecord(TestUtil.generateRecord(3), true, true);
//            table.insertRecord(TestUtil.generateRecord(4), true, true);
//            tm.commit();
//        });
//    }

}
