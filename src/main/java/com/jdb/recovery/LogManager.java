package com.jdb.recovery;

import com.jdb.storage.BufferPool;
import com.jdb.storage.Page;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.jdb.common.Constants.LOG_FILE_PATH;
import static com.jdb.common.Constants.PAGE_SIZE;

public class LogManager {
    private BufferPool bufferPool;
    private Deque<Integer> unflushedLogTail;
    private LogPage logTail;

    private int nextPID = 0;

    public LogManager() {
        bufferPool = BufferPool.getInstance();
        unflushedLogTail = new ArrayDeque<>();
    }

    static long makeLSN(int pid, int offset) {
        return (long) (pid) << Integer.SIZE | offset;
    }

    public synchronized long append(LogRecord log) {
        if (logTail == null || logTail.getFreeSpace() < log.getSize()) {
            logTail = new LogPage(bufferPool.newPage(LOG_FILE_PATH, nextPID++));
            logTail.init();
        }
        int offset = logTail.append(log);
        long lsn = makeLSN(nextPID - 1, offset);
        log.setLsn(lsn);
        return lsn;
    }

    public synchronized void flush2lsn(long lsn) {
        Iterator<Integer> iter = unflushedLogTail.iterator();
        int pid = getLSNPage(lsn);
        while (iter.hasNext()) {
            Integer unflushedPageId = iter.next();
            if (unflushedPageId > pid)
                break;
            bufferPool.flushPage(LOG_FILE_PATH, unflushedPageId);
            iter.remove();
        }
    }

    public LogRecord getLogRecord(long lsn) {
        LogPage logPage = new LogPage(bufferPool.getPage(LOG_FILE_PATH, getLSNPage(lsn)));
        return logPage.getLogRecord(getLSNOffset(lsn));
    }

    public Iterator<LogRecord> scanFrom(long lsn) {
        return new LogIterator(lsn);
    }

    private int getLSNPage(long lsn) {
        long pageId = lsn >> Integer.SIZE;
        return (int) pageId;
    }

    private int getLSNOffset(long lsn) {
        return (int) lsn & Integer.MAX_VALUE;
    }

    class LogIterator implements Iterator<LogRecord> {
        int pid;
        Iterator<LogRecord> internalIter;
        int nextPID;
        LogIterator(long lsn) {
            this.pid = getLSNPage(lsn);
            Page page = bufferPool.getPage(LOG_FILE_PATH, nextPID);
            this.pid++;

            LogPage logPage = new LogPage(page);
            internalIter = logPage.scanFrom(getLSNOffset(lsn));
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
                    Page page = bufferPool.getPage(LOG_FILE_PATH, nextPID);
                    LogPage logPage = new LogPage(page);
                    internalIter = logPage.scan();
                    nextPID++;
                } catch (RuntimeException e) {
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
        int PAGE_ID_OFFSET = 0;
        int RECORD_COUNT_OFFSET = PAGE_ID_OFFSET + Integer.BYTES;
        int TAIL_OFFSET = RECORD_COUNT_OFFSET + Integer.BYTES;
        int HEADER_SIZE = TAIL_OFFSET + Integer.BYTES;
        ByteBuffer buffer;
        Page page;

        public LogPage(Page page) {
            this.page = page;
            buffer = ByteBuffer.wrap(page.getData());
        }

        public void init() {
            buffer.putInt(PAGE_ID_OFFSET, getPageId());
            buffer.putInt(RECORD_COUNT_OFFSET, 0);
            setTail(HEADER_SIZE);
        }

        public int getFreeSpace() {
            return PAGE_SIZE - getTail();
        }

        public int getPageId() {
            return buffer.getInt(PAGE_ID_OFFSET);
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
                offset += log.getSize();
                return log;
            }
        }
    }
}
