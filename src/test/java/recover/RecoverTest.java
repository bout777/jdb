package recover;

import com.jdb.common.Value;
import com.jdb.recovery.LogRecord;
import com.jdb.recovery.LogType;
import com.jdb.recovery.RecoveryManager;
import com.jdb.recovery.logs.BeginLog;
import com.jdb.recovery.logs.CommitLog;
import com.jdb.table.Table;
import com.jdb.transaction.TransactionManager;
import index.MockTable;
import org.junit.Before;
import org.junit.Test;
import version.DeterministicRunner;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class RecoverTest {

    Table table;
    public RecoveryManager rm = RecoveryManager.getInstance();

    @Before
    public void init() {
        table = MockTable.getTable();
    }

    @Test

    public void testSimpleTrx() {
        var rowDate = MockTable.generateRecord(123);
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
            table.insertRecord(MockTable.generateRecord(123), true, true);
        });
        runner.run(1, () -> {
            tm.begin();
            table.insertRecord(MockTable.generateRecord(456), true, true);
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
    public void testRecover(){
        var rowbefore = MockTable.generateRecord(123);
        var rowafter = MockTable.generateRecord(123);
        var tm  = TransactionManager.getInstance();
        tm.begin();
        table.insertRecord(rowbefore, true, true);
        tm.commit();

        tm.begin();
        table.updateRecord(Value.ofInt(123), rowafter);
        tm.commit();
    }

    @Test
    public void testCheckpoint() {
        var rowDate = MockTable.generateRecord(123);
        table.insertRecord(rowDate, true, true);


    }
}
