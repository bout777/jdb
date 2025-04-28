package com.jdb.recovery;

import com.jdb.Engine;
import com.jdb.common.PageHelper;
import com.jdb.recovery.logs.LogRecord;
import com.jdb.recovery.logs.MasterLog;
import com.jdb.storage.BufferPool;
import com.jdb.storage.Page;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.jdb.common.Constants.*;

/*
 * 由于日志页有固定的fid，所以每页只用保存pno
 * */
public class LogManager {
    private Engine engine;
    public static int inCount = 0;

    private static final long MASTER_LOG_PAGE_ID = PageHelper.concatPid(LOG_FILE_ID, 0);
    private BufferPool bufferPool;
    private Deque<Integer> unflushedLogTail = new ArrayDeque<>();
    private LogPage logTail;

    private long nextPID = MASTER_LOG_PAGE_ID + 1;

    public LogManager(BufferPool bp) {
        bufferPool = bp;
    }

    public LogManager(Engine engine) {
        this.engine = engine;
    }

    public void injectDependency() {
        bufferPool = engine.getBufferPool();
    }

    public void init() {
        //init master
        bufferPool.newPage(LOG_FILE_ID, false);
        rewriteMasterLog(new MasterLog(NULL_LSN));
    }

    public void rewriteMasterLog(MasterLog masterLog) {

        var buffer = bufferPool.getPage(MASTER_LOG_PAGE_ID).getBuffer();
        masterLog.serialize(buffer, 0);
    }

    public MasterLog getMasterLog() {
        var buffer = bufferPool.getPage(MASTER_LOG_PAGE_ID).getBuffer();
        LogRecord masterLog = LogRecord.deserialize(buffer, 0);
        return (MasterLog) masterLog;
    }

    static long makeLSN(long pid, int offset) {
        return pid << Integer.SIZE | offset;
    }

    public synchronized long append(LogRecord log) {
        if (logTail == null || logTail.getFreeSpace() < log.getSize()) {
            logTail = new LogPage(bufferPool.newPage(LOG_FILE_ID, false));
            // todo 更新nextpid？
            logTail.init();
        }
        if(unflushedLogTail.isEmpty()|| logTail.getPageNo()!=unflushedLogTail.getLast())
            unflushedLogTail.addLast(logTail.getPageNo());
        int offset = logTail.append(log);
        long lsn = makeLSN(logTail.getPageId(), offset);
        log.setLsn(lsn);
 //       System.out.printf("incount: %d input page: %d,off: %d,log: %s \n,",++inCount, lsn>>32&Integer.MAX_VALUE,getLSNOffset(lsn), log.getPrevLsn());
        return lsn;
    }

    public synchronized void flush2lsn(long lsn) {
        Iterator<Integer> iter = unflushedLogTail.iterator();
        long pid = getLSNPage(lsn);
        while (iter.hasNext()) {
            int unflushPno = iter.next();
            long unflushPid = PageHelper.concatPid(LOG_FILE_ID, unflushPno);
            if (unflushPid > pid)
                break;
            bufferPool.flushPage(unflushPid);
            iter.remove();
        }
    }

    public LogRecord getLogRecord(long lsn) {
        LogPage logPage = new LogPage(bufferPool.getPage(getLSNPage(lsn)));
        var log = logPage.getLogRecord(getLSNOffset(lsn));
        log.setLsn(lsn);
//        System.out.printf("out page: %d,off: %d,log: %s \n,", lsn>>32&Integer.MAX_VALUE,getLSNOffset(lsn), log.getPrevLsn());
        return log;
    }

    public Iterator<LogRecord> scanFrom(long lsn) {
        if (lsn <= NULL_LSN)
            return scan();
        else
            return new LogIterator(lsn);
    }

    public Iterator<LogRecord> scan() {
        return new LogIterator();
    }

    private long getLSNPage(long lsn) {
        return lsn >> Integer.SIZE | (long) LOG_FILE_ID << Integer.SIZE;
    }

    private int getLSNOffset(long lsn) {
        return (int) lsn & Integer.MAX_VALUE;
    }

    class LogIterator implements Iterator<LogRecord> {
        long pid;
        Iterator<LogRecord> internalIter;
//        int nextPNO;

        LogIterator(long lsn) {

            this.pid = getLSNPage(lsn);
            Page page = bufferPool.getPage(pid);
            this.pid++;

            LogPage logPage = new LogPage(page);
            internalIter = logPage.scanFrom(getLSNOffset(lsn));
        }

        LogIterator() {
            this.pid = MASTER_LOG_PAGE_ID + 1;
            Page page;
            try {
                page = bufferPool.getPage(pid);
            } catch (NoSuchElementException e) {
                return;
            }
            pid++;
            LogPage logPage = new LogPage(page);
            internalIter = logPage.scan();
            while (!internalIter.hasNext()) {
                try {
                    page = bufferPool.getPage(this.pid);
                    logPage = new LogPage(page);
                    internalIter = logPage.scan();
                    this.pid++;
                } catch (NoSuchElementException e) {
                    internalIter = null;
                    break;
                }
            }
        }

        @Override
        public boolean hasNext() {
            return internalIter != null;
        }

        @Override
        public LogRecord next() {
            if (!hasNext())
                throw new NoSuchElementException();

            LogRecord log = internalIter.next();
            while (!internalIter.hasNext()) {
                try {
                    Page page = bufferPool.getPage(this.pid);
                    LogPage logPage = new LogPage(page);
                    internalIter = logPage.scan();
                    this.pid++;
                    //todo ->
                } catch (NoSuchElementException e) {
                    internalIter = null;
                    break;
                }
            }
            return log;
        }
    }

    /**
     * [pageId] | [recordCount] | [tail] | [logRecord]...
     */
    class LogPage {
        static final int PAGE_NO_OFFSET = 0;
        static final int RECORD_COUNT_OFFSET = PAGE_NO_OFFSET + Integer.BYTES;
        static final int TAIL_OFFSET = RECORD_COUNT_OFFSET + Integer.BYTES;
        static final int HEADER_SIZE = TAIL_OFFSET + Integer.BYTES;
        ByteBuffer buffer;
        Page page;

        public LogPage(Page page) {
            this.page = page;
            buffer = page.getBuffer();
        }

        public void init() {
            buffer.putInt(PAGE_NO_OFFSET, PageHelper.getPno(page.pid));
            buffer.putInt(RECORD_COUNT_OFFSET, 0);
            setTail(HEADER_SIZE);
        }

        public int getFreeSpace() {
            return PAGE_SIZE - getTail();
        }

        public long getPageId() {
            return PageHelper.concatPid(LOG_FILE_ID, getPageNo());
        }

        public int getPageNo() {
            return buffer.getInt(PAGE_NO_OFFSET);
        }

        public int append(LogRecord log) {
            int tail = getTail();
            int newTail = log.serialize(buffer, tail);
            setTail(newTail);
            return tail;
        }

        public LogRecord getLogRecord(int offset) {
            return LogRecord.deserialize(buffer, offset);
        }

        public Iterator<LogRecord> scanFrom(int offset) {
            return new InternalLogIterator(offset);
        }

        public Iterator<LogRecord> scan() {
            return new InternalLogIterator();
        }

        public int getTail() {
            return buffer.getInt(TAIL_OFFSET);
        }

        public void setTail(int tail) {
            buffer.putInt(TAIL_OFFSET, tail);
        }

        class InternalLogIterator implements Iterator<LogRecord> {
            int offset;

            public InternalLogIterator(int offset) {
                this.offset = offset;
            }

            public InternalLogIterator() {
                this.offset = HEADER_SIZE;
            }

            @Override
            public boolean hasNext() {
                return offset >= HEADER_SIZE && offset < getTail();
            }

            @Override
            public LogRecord next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                LogRecord log = LogRecord.deserialize(buffer, offset);
                long lsn = makeLSN(getPageId(), offset);
                log.setLsn(lsn);
                offset += log.getSize();
            //    System.out.printf("iter out page: %d,off: %d,log: %s \n,", lsn>>32&Integer.MAX_VALUE,getLSNOffset(lsn), log.getPrevLsn());
                return log;
            }
        }
    }

    //
}
