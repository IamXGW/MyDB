package com.iamxgw.mydb.backend.dm.pageIndex;

import com.iamxgw.mydb.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 页面索引，缓存了每一页的空闲空间。用于在上层模块进行插入操作时，
 * 能够快速找到一个合适空间的页面，而无需从磁盘或者缓存中检查每一个页面的信息。
 */
public class PageIndex {
    // 一页划分为 40 个区块
    private static final int INTERVALS_NO = 40;

    // 每个区块的大小
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;
    private Lock lock;
    // 记录空闲区块恰有 x 个的页面都有谁
    private List<PageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO + 1];
        for (int i = 0; i < INTERVALS_NO + 1; ++i) {
            lists[i] = new ArrayList<>();
        }
    }

    /**
     * 将空闲大小为 freeSpace 大小的页，放入到 PageIndex 中
     * @param pgno
     * @param freeSpace
     */
    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 选择一个恰有 number 个空前区块的页面，并返回。
     * 如果当前不包含这样的页，那就将 number + 1，再去尝试
     * 注意：同一个页面，不允许并发的写。当上层模块使用完该页后，会重新插入到 PageIndex 中
     * @param spaceSize
     * @return
     */
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            // spaceSize 需要的区块的个数，向上取整
            int number = spaceSize / THRESHOLD;
            if (number < INTERVALS_NO) number++;
            while (number <= INTERVALS_NO) {
                if (lists[number].size() == 0) {
                    number++;
                    continue;
                }
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }
}
