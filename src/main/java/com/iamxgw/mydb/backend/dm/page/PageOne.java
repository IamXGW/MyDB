package com.iamxgw.mydb.backend.dm.page;

import com.iamxgw.mydb.backend.dm.pageCache.PageCache;
import com.iamxgw.mydb.backend.utils.RandomUtil;

import java.util.Arrays;

/**
 * 特殊的第一页
 * ValidCheck
 * MyDB 启动时，会在 100-107 字节处填入一个随机字节，当 MyDB 关闭时，会将其拷贝到第 108-115 字节
 * 用于判断数据库上次是不是正常关闭
 */
public class PageOne {
    private static final int OF_VC = 100;
    private static final int LEN_VC = 8;

    public static byte[] InitRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }

    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC + LEN_VC, LEN_VC);
    }

    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    private static boolean checkVc(byte[] raw) {
        // JDK 11 use
        return Arrays.compare(raw, OF_VC, OF_VC + LEN_VC, raw
                , OF_VC + LEN_VC, OF_VC + 2 * LEN_VC) == 0;
        // JDK 8 use
//        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC + LEN_VC),
//                Arrays.copyOfRange(raw, OF_VC + LEN_VC, OF_VC + 2 * LEN_VC));
    }
}
