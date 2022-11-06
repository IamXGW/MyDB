package com.iamxgw.mydb.backend.dm.dataItem;

import com.iamxgw.mydb.backend.dm.DataManagerImpl;
import com.iamxgw.mydb.backend.dm.page.Page;
import com.iamxgw.mydb.common.SubArray;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataItemImpl implements DataItem {

    static final int OF_VALID = 0;
    static final int OF_SIZE = 1;
    static final int OF_DATA = 3;

    private SubArray raw;
    private byte[] oldRaw;
    private Lock rLock;
    private Lock wLock;
    private DataManagerImpl dm;
    private long uid;
    private Page pg;

    public DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        this.dm = dm;
        this.uid = uid;
        this.pg = pg;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
    }

    public boolean isValid() {
        return raw.raw[raw.start + OF_VALID] == (byte)0;
    }

    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start + OF_DATA, raw.end);
    }

    /**
     * 在修改之前需要调用 before 方法
     */
    @Override
    public void before() {
        wLock.lock();
        pg.setDirty(true);
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    /**
     * 想要撤销修改，需要调用 unBefore 方法
     */
    @Override
    public void unBefore() {
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wLock.unlock();
    }

    /**
     * 修改完成后，调用 after 方法
     * @param xid
     */
    @Override
    public void after(long xid) {
        dm.logDataItem(xid, this);
        wLock.unlock();
    }

    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return pg;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }
}
