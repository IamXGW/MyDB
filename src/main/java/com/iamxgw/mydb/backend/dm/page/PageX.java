package com.iamxgw.mydb.backend.dm.page;

import com.iamxgw.mydb.backend.dm.pageCache.PageCache;
import com.iamxgw.mydb.backend.utils.Parser;

import java.util.Arrays;

/**
 * PageX 管理普通页面
 * 普通页结构如下：
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset 大小为 2 字节，表示页空闲位置开始的偏移
 */
public class PageX {
    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw, OF_DATA);
        return raw;
    }

    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }

    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, OF_FREE, OF_DATA));
//        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    /**
     * 将 raw 数据插入到 pg 中，并返回插入位置。注意：插入数据不可以超过页大小！
     * @param pg
     * @param raw
     * @return
     */
    public static short insert(Page pg, byte[] raw) {
        pg.setDirty(true);
        short offset = getFSO(pg.getData());
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        setFSO(pg.getData(), (short)(offset + raw.length));
        return offset;
    }

    /**
     * 获取 pg 剩余页的大小
     * @param pg
     * @return
     */
    public static int getFreeSpace(Page pg) {
        return PageCache.PAGE_SIZE - (int)getFSO(pg.getData());
    }

    /**
     * 将 raw 数据插入到 offset 位置，并将 pg 的 offset 更新（取较大的 offset）
     * @param pg
     * @param raw
     * @param offset
     */
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        short rawFSO = getFSO(pg.getData());
        if (rawFSO < offset + raw.length) {
            setFSO(pg.getData(), (short)(offset + raw.length));
        }
    }

    /**
     * 将 raw 数据插入到 offset 位置，但是不更新 offset
     * @param pg
     * @param raw
     * @param offset
     */
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }
}
