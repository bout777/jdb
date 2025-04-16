package com.jdb.recovery;

import com.jdb.recovery.logs.*;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Page;
import com.jdb.table.PagePointer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.jdb.common.Constants.*;

/**
 * 负责日志管理
 * 追加日志
 * 将日志刷入磁盘
 * 根据xid回滚事务
 */
public class RecoveryManager {
    private BufferPool bufferPool;
    //private List<LogRecord> logBuffer;
    private LogManager logManager;
    //脏页表 <pid->recLsn>
    private Map<Long, Long> dirtyPagesTable = new ConcurrentHashMap<>();
    //活跃事务表 <xid->lsn>
    private Map<Long, Long> transactionsTable = new ConcurrentHashMap<>();

    public RecoveryManager() {
      //  logBuffer = new ArrayList<>();

    }

    public void setLogManager(LogManager logManager) {
        this.logManager = logManager;
    }

    public LogManager getLogManager() {
        return logManager;
    }



    public static RecoveryManager instance = new RecoveryManager();



    public static RecoveryManager getInstance() {
        return instance;
    }

    public synchronized void registerTransaction(long xid) {
        transactionsTable.put(xid, 0L);
        logTrxBegin(xid);
    }

    //====== 追加日志 ======//
    public void logTrxBegin(long xid) {
        LogRecord log = new BeginLog(xid);
        long lsn = logManager.append(log);
        transactionsTable.put(xid, lsn);
    }

    public void logUpdate(long xid, int pid, short offset, byte[] oldData, byte[] newData) {
        LogRecord log = new UpdateLog(xid, pid, offset, offset,oldData, newData);
        logManager.append(log);
    }

    public void logUndoCLR(long undoNextLsn) {
        logManager.append(new CompensationLog(undoNextLsn));
    }

    public synchronized long logInsert(long xid, PagePointer ptr, byte[] image) {
        assert transactionsTable.containsKey(xid);

        //获得事务表中对应事务的lastLsn，作为下一个日志记录的prevLsn
        long prevLsn = transactionsTable.get(xid);
        LogRecord log = new InsertLog(xid, prevLsn, ptr, image);

        //追加日志
        long lsn = logManager.append(log);

        //更新活跃事务表
        transactionsTable.put(xid, lsn);

        //更新脏页表
        dirtyPage(ptr.pid, lsn);
        return lsn;
    }

    public void logCommit(long xid) {
        LogRecord log = new CommitLog(xid);
        long lsn = logManager.append(log);
        logManager.flush2lsn(lsn);
        transactionsTable.remove(xid);
    }
    public void logAbort(long xid) {
        long lastLsn = transactionsTable.remove(xid);
        LogRecord log = new AbortLog(xid,lastLsn);
        long lsn = logManager.append(log);
    }

    public void flush2lsn(long lsn) {
        logManager.flush2lsn(lsn);
    }

    public void dirtyPage(long pid, long lsn) {
        dirtyPagesTable.putIfAbsent(pid, lsn);
        dirtyPagesTable.computeIfPresent(pid, (k, v) -> Math.min(v, lsn));
    }


    private void rollback2lsn(long xid, long lsn) {
        var lastRecord = logManager.getLogRecord(transactionsTable.get(xid));
        long curLsn;
        if (lastRecord instanceof CompensationLog clr) {
            curLsn = clr.getUndoNextLsn();
        } else {
            curLsn = lastRecord.getLsn();
        }

        while (curLsn > lsn) {
            var log = logManager.getLogRecord(curLsn);
            if (log instanceof CompensationLog clr) {
                curLsn = clr.getUndoNextLsn();
                continue;
            }
            
            curLsn = log.getPrevLsn();
            logUndoCLR(curLsn);

            log.undo();
        }
    }

    //在recover中的每次日志写入都要刷盘！

    /*
     * 最简单的实现就是undo时直接追加一条补偿记录
     * 然后按照redo，undo阶段来执行
     * 是可以保证一致性的*/

    public synchronized void checkpoint() {
        //先实现只写一个检查点日志的版本，暂不考虑脏页表和事务表的数据超过单个日志页容量的情况

        var checkpointLog = new CheckpointLog(dirtyPagesTable, transactionsTable);
        long lsn = logManager.append(checkpointLog);

        flush2lsn(lsn);
    }

    public void recover() {

    }
//    long redoLsn;

    public void analyze() {

        /*TODO
         *  找到一个合适的，足够小的redolsn，保证该lsn之前的log已经不需要处理
         *  即redolsn之前的对数据库更改已经持久化*/

        /*TODO
         *  从redolsn开始往后扫描，维护一个undo-list
         *  将每个扫描到的log中的事务添加到undo-list中
         *  如果遇到commit或者abort记录，将该事务从undo-list中移出
         *  在扫描过程中，如果有update记录的页不在脏页表里，添加该页到脏页表*/
        MasterLog masterLog = logManager.getMasterLog();
        long lastCheckpointLsn = masterLog.getLastCheckpointLsn();
        var logIter = logManager.scanFrom(lastCheckpointLsn);
        var checkpointLog = (CheckpointLog) logIter.next();

        this.dirtyPagesTable = checkpointLog.getDirtyPageTable();
        this.transactionsTable = checkpointLog.getActiveTransactionTable();

        while (logIter.hasNext()) {
            var log = logIter.next();
            if (log.getXid() != NULL_XID) {
                long xid = log.getXid();
                transactionsTable.putIfAbsent(xid, log.getLsn());
                transactionsTable.computeIfPresent(xid, (k, v) -> Math.max(v, log.getLsn()));
            }
            if (log.getPageId() != NULL_PAGE_ID) {
                dirtyPage(log.getPageId(), log.getLsn());
            }
            if (log.getType() == LogType.COMMIT || log.getType() == LogType.ABORT) {
                transactionsTable.remove(log.getXid());
            }
        }

//        redoLsn = Collections.min(dirtyPagesTable.values());
    }

    public void redo() {
        long redoLsn = Collections.min(dirtyPagesTable.values());
        var logIter = logManager.scanFrom(redoLsn);
        while (logIter.hasNext()) {
            var log = logIter.next();
            var recLsn = dirtyPagesTable.get(log.getPageId());
            if (recLsn == null || log.getLsn() < recLsn)
                continue;

            Page page = bufferPool.getPage(log.getPageId());
            if (log.getLsn() <= page.getLsn())
                continue;

            log.redo();
        }
    }

    public void undo() {
        /*TODO
         *  维护一个undo-list优先队列，每次循环取出lsn最大的事务
         *  根据lsn取出log，如果是一个updatelog，执行undo动作，并追加一个clr记录
         *  如果是一个clr，将它指向的undonext加入队列中，进行下一轮循环*/
        record entry(long xid, long lsn) {
        }

        var undoList = new PriorityQueue<entry>((e1, e2) -> Long.compare(e2.lsn, e1.lsn));
        for (var mapEntry : transactionsTable.entrySet()) {
            undoList.add(new entry(mapEntry.getKey(), mapEntry.getValue()));
        }
        while (!undoList.isEmpty()) {
            var entry = undoList.poll();
            long xid = entry.xid;
            long lsn = entry.lsn;
            var log = logManager.getLogRecord(lsn);
            long nextLsn = NULL_LSN;
            if (log instanceof CompensationLog clr) {
                nextLsn = clr.getUndoNextLsn();
            } else {
                log.undo();
                nextLsn = log.getPrevLsn();
                logManager.append(new CompensationLog(nextLsn));
            }

            if (nextLsn == NULL_LSN) {
                long lastLsn = transactionsTable.remove(xid);
                logManager.append(new AbortLog(xid,lastLsn));
            } else {
                undoList.add(entry);
            }
        }
    }

//    public LogRecord getLog(long lsn) {
//        return null;
//    }

    public int getNextLsn() {
        return 0;
    }
}
