package com.jdb.recover;

import com.jdb.TestUtil;
import com.jdb.common.Value;
import com.jdb.recovery.LogType;
import com.jdb.recovery.RecoveryManager;
import com.jdb.recovery.logs.BeginLog;
import com.jdb.recovery.logs.CommitLog;
import com.jdb.recovery.logs.LogRecord;
import com.jdb.table.Table;
import com.jdb.transaction.TransactionContext;
import com.jdb.transaction.TransactionManager;
import org.junit.Before;
import org.junit.Test;
import com.jdb.version.DeterministicRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;

public class RecoverTest {

    Table table;
    public RecoveryManager rm = RecoveryManager.getInstance();

    @Before
    public void init() {
        table = Table.getTestTable();
    }

    @Test

    public void testSimpleTrx() {
        var rowDate = TestUtil.generateRecord(123);
        TransactionManager.getInstance().begin();
        table.insertRecord(rowDate, true, true);
        TransactionManager.getInstance().commit();
        var iter = rm.getLogManager().scan();

        assertEquals(iter.next(), new BeginLog(1));
        assertEquals(iter.next().getType(), LogType.INSERT);
        assertEquals(iter.next(), new CommitLog(1));
        assertFalse(iter.hasNext());
    }

    @Test
    public void testMultipleTrx() {
        var runner = new DeterministicRunner(2);
        List<LogRecord> expect = new ArrayList<>();
        var tm = TransactionManager.getInstance();
        runner.run(0, () -> {
            tm.begin();
            table.insertRecord(TestUtil.generateRecord(123), true, true);
        });
        runner.run(1, () -> {
            tm.begin();
            table.insertRecord(TestUtil.generateRecord(456), true, true);
        });
        runner.run(1, () -> {
            tm.commit();
        });
        runner.run(0, () -> {
            tm.commit();
        });

        var iter = rm.getLogManager().scan();
        assertEquals(new BeginLog(1), iter.next());
        assertEquals(LogType.INSERT, iter.next().getType());
        assertEquals(new BeginLog(2), iter.next());
        assertEquals(LogType.INSERT, iter.next().getType());
        assertEquals(new CommitLog(2), iter.next());
        assertEquals(new CommitLog(1), iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    public void testRecover() {
        var rowbefore = TestUtil.generateRecord(123);
        var rowafter = TestUtil.generateRecord(123);
        var tm = TransactionManager.getInstance();
        tm.begin();
        table.insertRecord(rowbefore, true, true);
        tm.commit();

        tm.begin();
        table.updateRecord(Value.ofInt(123), rowafter, true);
        tm.commit();
    }

    @Test
    public void testSimpleUndo() {
        var tm = TransactionManager.getInstance();
        var runner = new DeterministicRunner(1);
        runner.run(0, () -> {
            tm.begin();
            table.insertRecord(TestUtil.generateRecord(123), true, true);
            long xid = TransactionContext.getTransaction().getXid();
            rm.rollback(xid);
        });
        tm.begin();
        try{
            table.getClusterIndex().searchEqual(Value.ofInt(123));
            fail();
        }catch (NoSuchElementException e){

        }


    }

    @Test
    public void testAbort() {
        var tm = TransactionManager.getInstance();
        var rowbefore = TestUtil.generateRecord(123);
        var rowafter = TestUtil.generateRecord(123);
        var rm = RecoveryManager.getInstance();
        var runner = new DeterministicRunner(1);
        runner.run(0, () -> {
            tm.begin();
            table.insertRecord(rowbefore, true, true);
            table.updateRecord(Value.ofInt(123), rowafter, true);
            long xid = TransactionContext.getTransaction().getXid();
            rm.rollback(xid);
        });
        var lm = rm.getLogManager();
        var iter = lm.scan();
        assertEquals(new BeginLog(1), iter.next());
        assertEquals(LogType.INSERT, iter.next().getType());
        assertEquals(LogType.DELETE, iter.next().getType());
        assertEquals(LogType.INSERT, iter.next().getType());
        assertEquals(LogType.DELETE, iter.next().getType());
        assertEquals(LogType.INSERT, iter.next().getType());
        assertEquals(LogType.DELETE, iter.next().getType());
        assertFalse(iter.hasNext());
    }

    @Test
    public void testCheckpoint() {
        var tm = TransactionManager.getInstance();
        var runner = new DeterministicRunner(3);
        runner.run(0, () -> {
            tm.begin();
            table.insertRecord(TestUtil.generateRecord(1), true, true);
            table.insertRecord(TestUtil.generateRecord(2), true, true);
            tm.commit();
        });
        runner.run(1, () -> {
            tm.begin();
            table.insertRecord(TestUtil.generateRecord(3), true, true);
            table.insertRecord(TestUtil.generateRecord(4), true, true);
            tm.commit();
        });
    }

}
