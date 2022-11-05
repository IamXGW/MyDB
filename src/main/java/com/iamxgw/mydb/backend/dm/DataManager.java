package com.iamxgw.mydb.backend.dm;

import com.iamxgw.mydb.backend.dm.dataItem.DataItem;
import com.iamxgw.mydb.backend.dm.logger.Logger;
import com.iamxgw.mydb.backend.dm.page.PageOne;
import com.iamxgw.mydb.backend.dm.pageCache.PageCache;
import com.iamxgw.mydb.backend.tm.TransactionManager;

/**
 * DM 直接管理数据库 DB 文件和日志文件
 * DM 的主要职责有：
 *  1) 分页管理 DB 文件，并进行缓存
 *  2) 管理日志文件，保证在发生错误时可以根据日志进行恢复
 *  3) 抽象 DB 文件为 DataItem 供上层模块使用，并提供缓存
 */
public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();

    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();
        return dm;
    }

    public static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        if (!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }
        dm.fillPageIndex();
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);

        return dm;
    }
}
