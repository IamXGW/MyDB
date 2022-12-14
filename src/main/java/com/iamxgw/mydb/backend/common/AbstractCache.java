package com.iamxgw.mydb.backend.common;

import com.iamxgw.mydb.common.Error;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 使用「引用计数」作为缓存的淘汰策略
 * @param <T>
 */
public abstract class AbstractCache<T> {
    // 实际缓存的数据
    private HashMap<Long, T> cache;
    // 资源的引用个数
    private HashMap<Long, Integer> references;
    // 正在被获取的资源
    // 应多对线程场景，如果当前有其他线程正在获取该缓存，那当前线程就等会（1 million）再过来获取
    private HashMap<Long, Boolean> getting;

    // 最大允许缓存资源数
    private int maxResource;
    // 缓存中元素个数
    private int count = 0;
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 获取 key 中的缓存内容
     * @param key
     * @return
     * @throws Exception
     */
    protected T get(long key) throws Exception {
        while (true) {
            lock.lock();
            // 该资源是否正在被使用
            if (getting.containsKey(key)) {
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }
            // 缓存中是否有该资源
            if (cache.containsKey(key)) {
                T obj = cache.get(key);
                references.put(key, references.getOrDefault(key, 0) + 1);
                lock.unlock();
                return obj;
            }

            // 尝试获取资源
            if (maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw Error.CacheFullException;
            }
            count++;
            getting.put(key, true);
            lock.unlock();
            break;
        }

        T obj = null;
        try {
            obj = getForCache(key);
        } catch (Exception e) {
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();

        return obj;
    }

    /**
     * 强制释放一个缓存
     * @param key
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key) - 1;
            if (ref == 0) {
                T obj = cache.get(key);
                releaseForCache(obj);
                cache.remove(key);
                count--;
            } else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，并将所有缓存数据写回
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 当资源不在缓存时，获取行为
     * @param key
     * @return
     * @throws Exception
     */
    protected  abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时的写会行为
     * @param obj
     */
    protected abstract void releaseForCache(T obj);
}
