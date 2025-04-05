package com.jdb.recovery;

import com.jdb.recovery.logs.BeginLog;
import com.jdb.recovery.logs.CommitLog;
import com.jdb.recovery.logs.CompensationLog;
import com.jdb.recovery.logs.UpdateLog;
import com.jdb.storage.BufferPool;
import com.jdb.table.DataPage;
import com.jdb.table.Record;
import com.jdb.table.RecordID;
import com.jdb.transaction.Transaction;

import java.util.*;

import static com.jdb.common.Constants.*;

/**
 * 负责日志管理
 * 追加日志
 * 将日志刷入磁盘
 * 根据xid回滚事务
 */
public class RecoveryManager {
    private BufferPool bufferPool;
    private List<LogRecord> logBuffer;

    //脏页表 <tablePagePtr,recLsn>
    private Map<TablePageInfo, Integer> dirtyPagesTable;
    //活跃事务表 <xid,lsn>
    private Map<Integer, Integer> transactionsTable;

    public RecoveryManager() {
        logBuffer = new ArrayList<>();
    }

    public void append(LogRecord logRecord) {
        logBuffer.add(logRecord);
    }

    public void appendBeginLog(long xid) {
        LogRecord log = new BeginLog(xid);
        append(log);
    }

    public void appendUpdateLog(long xid, int pid, short offset, byte[] oldData, byte[] newData) {
//        UpdateLog log = new UpdateLog(xid, pid, offset, oldData, newData);
//        append(log);
    }

    public void appendInsertLog(long xid, RecordID rid, Record record) {
    }

    public void appendCommitLog(long xid) {
        LogRecord log = new CommitLog(xid);
        append(log);
    }


    public void flush() {
    }

    //这个函数应该迁移到TransactionManager
    public void rollback(long xid) {
        /*TODO
         *  找出该事务的lsn
         *  取出log
         *  沿着prevLsn指针往前undo到NULL_LSN*/
        for (int i = logBuffer.size() - 1; i >= 0; i--) {
            LogRecord log = logBuffer.get(i);
            if (log.getXid() == xid) {
                log.undo();
            }
        }
    }

    //在recover中的每次日志写入都要刷盘！

    /*
     * 最简单的实现就是undo时直接追加一条补偿记录
     * 然后按照redo，undo阶段来执行
     * 是可以保证一致性的*/
    public void recover() {

    }

    public void analyze() {
        int redoLsn;
        /*TODO
         *  找到一个合适的，足够小的redolsn，保证该lsn之前的log已经不需要处理
         *  即redolsn之前的对数据库更改已经持久化*/

        /*TODO
         *  从redolsn开始往后扫描，维护一个undo-list
         *  将每个扫描到的log中的事务添加到undo-list中
         *  如果遇到commit或者abort记录，将该事务从undo-list中移出
         *  在扫描过程中，如果有update记录的页不在脏页表里，添加该页到脏页表*/
    }

    public void redo(int redoLsn, List<Integer> undoList, Map<Integer, Transaction> att, List<Integer> dirtyPages) {
        int n = getNextLsn();
        for (int i = redoLsn; i < n; i++) {
            LogRecord log = getLog(i);
            /*
             * TODO
             *  取出log的pageId，如果该页不在脏页表中，跳过
             *  else if-页的reclsn>i,说明这个log所记录的更改已经持久化，跳过
             *  else 从磁盘取出该页来，if lsn>i,跳过
             *  else 执行redo，对于update和clr的redo逻辑不同，在对应类文件中说明
             *  */

            LogType type = log.getType();
            int pageId = log.getPageId();
            if (pageId == NULL_PAGE_ID)
                continue;
            if (!dirtyPages.contains(pageId))
                continue;

            //TODO 查询页表中对应页的reclsn

            DataPage dataPage = new DataPage( BufferPool.getInstance().getPage(LOG_FILE_PATH, pageId));
            if (dataPage.getLsn() > i) {
                continue;
            }

            log.redo();
        }
    }

//    public void undo() {
//        /*TODO
//         *  维护一个undo-list优先队列，每次循环取出lsn最大的事务
//         *  根据lsn取出log，如果是一个updatelog，执行undo动作，并追加一个clr记录
//         *  如果是一个clr，将它指向的undonext加入队列中，进行下一轮循环*/
//        Queue<Integer> undoList = new PriorityQueue<>();
//
//        while (!undoList.isEmpty()) {
//            long lsn = undoList.poll();
//            LogRecord log = getLog(lsn);
//            if (log.getType() == LogType.COMPENSATION) {
//                int undoNextLsn = ((CompensationLog) log).getUndoNextLsn();
//                undoList.add(undoNextLsn);
//                continue;
//            }
//
//            log.undo();
//
//            long prevLsn = log.getPrevLsn();
//            if (prevLsn != NULL_LSN)
//                undoList.add(log.getPrevLsn());
//        }
//    }

    public LogRecord getLog(long lsn) {
        return null;
    }

    public int getNextLsn() {
        return 0;
    }
}

class TablePageInfo {
    private int tableId;
    //已经被刷到磁盘的最大lsn
    private int pageId;
}
