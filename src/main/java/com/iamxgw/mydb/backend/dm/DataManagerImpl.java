package com.iamxgw.mydb.backend.dm;

import com.iamxgw.mydb.backend.common.AbstractCache;
import com.iamxgw.mydb.backend.dm.dataItem.DataItem;
import com.iamxgw.mydb.backend.dm.dataItem.DataItemImpl;
import com.iamxgw.mydb.backend.dm.logger.Logger;
import com.iamxgw.mydb.backend.dm.page.Page;
import com.iamxgw.mydb.backend.dm.page.PageOne;
import com.iamxgw.mydb.backend.dm.pageCache.PageCache;
import com.iamxgw.mydb.backend.dm.pageIndex.PageIndex;
import com.iamxgw.mydb.backend.dm.pageIndex.PageInfo;
import com.iamxgw.mydb.backend.tm.TransactionManager;
import com.iamxgw.mydb.backend.dm.page.PageX;
import com.iamxgw.mydb.backend.utils.Panic;
import com.iamxgw.mydb.backend.utils.Types;
import com.iamxgw.mydb.common.Error;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.tm = tm;
        this.pc = pc;
        this.logger = logger;
        this.pIndex = new PageIndex();
    }

    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl) super.get(uid);
        if (!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if (raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        PageInfo pi = null;
        for (int i = 0; i < 5; ++i) {
            pi = pIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                int newPgno = pc.newPage(PageX.initRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        if (pi == null) {
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            pg = pc.getPage(pi.pgno);
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);

            short offset = PageX.insert(pg, raw);

            pg.release();
            return Types.addressToUid(pi.pgno, offset);
        } finally {
            // 将取出的 pg 重新插入 pIndex
            if (pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    /**
     * 在缓存中获取 uid 对应的 DataItem
     * uid 组成为 [pgno offset]
     * 其中，pgno 和 offset 各占 4byte
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int) (uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    /**
     * DataItem 缓存释放
     * @param di
     */
    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    // 在创建文件时初始化 PageOne
    void initPageOne() {
        int pgno = pc.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    // 在打开已有文件时，需要读入 PageOne，并验证 PageOne 的正确性
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    /**
     * 初始化 PageIndex
     * 打开已有 DM 时，需要将 PageIndex 刷一遍
     */
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for (int i = 2; i <= pageNumber; ++i) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }

    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }
}
