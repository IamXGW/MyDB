package com.iamxgw.mydb.backend.tm;

import com.iamxgw.mydb.backend.utils.Panic;
import com.iamxgw.mydb.backend.utils.Parser;
import com.iamxgw.mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * [Header] [TX1] [TX2] ... [TXn]
 * Header：8 字节长度，记录该 xid 文件管理事务的个数
 * TX：1 字节长度，记录当前事务的状态，0 活跃，1 已提交，2 取消
 */
public class TransactionManagerImpl implements TransactionManager {
    // XID 头文件长度
    static final int LEN_XID_HEADER_LENGTH = 8;
    // 每个事务占用的长度
    private static final int XID_FIELD_SIZE = 1;
    // 事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED = 2;
    // 超级事务，永远是 committed 状态
    public static final long SUPER_XID = 0;
    // XID 文件后缀
    static final String XID_SUFFIX = ".xid";

    private RandomAccessFile file;
    private FileChannel fc;
    // 记录事务的数量
    private long xidCounter;
    private Lock counterLock;

    public TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 检查 XID 文件的合法性
     * 原理：获取 XID_FILE_HEADER 中 xidcounter，根据它计算文件的理论长度，并和实际长度做对比
     */
    private void checkXIDCounter() {
        long fileLen = 0;
        try {
            fileLen = file.length();

        } catch (IOException e1) {
            Panic.panic(e1);
        }
        if (fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BadLogFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.xidCounter = Parser.parseLong(buf.array());
        long end = getXidPosition(this.xidCounter + 1);
        if (end != fileLen) {
            Panic.panic(Error.BadXIDFileException);
        }
    }

    /**
     * 获取事务的位置
     * @param xid
     * @return
     */
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }

    /**
     * 开启一个事务，并返回 XID
     * @return 事务 id，XID
     */
    @Override
    public long begin() {
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            incrXIDCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    /**
     * 将 XID 加一，并更新 XID Header
     */
    private void incrXIDCounter() {
        xidCounter++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     *更新事务状态为 status
     * @param xid
     * @param status
     */
    private void updateXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 提交 xid 事务
     * @param xid
     */
    @Override
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    /**
     * 回滚 xid 事务
     * @param xid
     */
    @Override
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    /**
     * 检查 xid 事务的状态是不是 status
     * @param xid
     * @param status
     * @return
     */
    private boolean checkXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }
    @Override
    public boolean isActivate(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
