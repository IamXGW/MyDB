package com.iamxgw.mydb.backend.dm.page;

/**
 * 存在内存中的页，区别已持久化到磁盘的抽象页
 */
public interface Page {
    void lock();
    void unlock();
    void release();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    byte[] getData();
}
