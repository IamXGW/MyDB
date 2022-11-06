package com.iamxgw.mydb.backend.dm.pageCache;

import com.iamxgw.mydb.backend.common.AbstractCache;
import com.iamxgw.mydb.backend.dm.page.Page;
import com.iamxgw.mydb.backend.dm.page.PageImpl;
import com.iamxgw.mydb.backend.utils.Panic;
import com.iamxgw.mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {
    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;

    // 记录当前打开的数据库文件有多少页，在数据库文件被打开时计算，在新建页面时自增
    private AtomicInteger pageNumbers;

    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResources) {
        super(maxResources);
        if (maxResources < MEM_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int)length / PAGE_SIZE);
    }

    /**
     * 新建一个 page，并返回其 pgno
     * @param initData
     * @return
     */
    @Override
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet();
        Page pg = new PageImpl(pgno, initData, null);
        flush(pg);
        return pgno;
    }

    @Override
    public Page getPage(int pgno) throws Exception {
        return get((long)pgno);
    }

    @Override
    public void close() {

    }

    /**
     * 根据 pgno 从数据库文件中读取页数据，并包裹成 Page
     * @param key
     * @return
     * @throws Exception
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int)key;
        long offset = PageCacheImpl.pageOffset(pgno);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();
        return new PageImpl(pgno, buf.array(), this);
    }

    /**
     * 驱逐页 pg，如果是脏页，则写回文件系统
     * @param pg
     */
    @Override
    protected void releaseForCache(Page pg) {
        if (pg.isDirty()) {
            flush(pg);
            pg.setDirty(false);
        }
    }

    /**
     * 将页 pg 写回文件系统
     * @param pg
     */
    private void flush(Page pg) {
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);

        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }

    @Override
    public void release(Page pg) {
        release((long)pg.getPageNumber());
    }

    @Override
    public void truncateByBgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page pg) {
        flush(pg);
    }

    private static int pageOffset(int pgno) {
        return (pgno - 1) * PAGE_SIZE;
    }
}
