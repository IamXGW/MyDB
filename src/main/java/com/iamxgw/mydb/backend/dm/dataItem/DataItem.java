package com.iamxgw.mydb.backend.dm.dataItem;

import com.google.common.primitives.Bytes;
import com.iamxgw.mydb.backend.dm.DataManagerImpl;
import com.iamxgw.mydb.backend.dm.page.Page;
import com.iamxgw.mydb.backend.utils.Parser;
import com.iamxgw.mydb.backend.utils.Types;
import com.iamxgw.mydb.common.SubArray;

import java.util.Arrays;

/**
 * dataItem 结构：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlat 1 字节，0 表示合法，1 表示非法。删除一个 DataItem，只需要简单地将其有效位设置为 0
 * DataSize 2 字节，标识 Data 长度
 */
public interface DataItem {
    // 共享数组 SubArray
    SubArray data();

    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short)raw.length);
        return Bytes.concat(valid, size, raw);
    }

    // 从页面的 offset 处解析此处的 dataitem
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItemImpl.OF_SIZE, offset + DataItemImpl.OF_DATA));
        short length = (short) (size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset + length), new byte[length], pg, uid, dm);
    }

    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte) 1;
    }
}
