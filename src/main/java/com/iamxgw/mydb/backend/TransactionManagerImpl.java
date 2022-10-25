package com.iamxgw.mydb.backend;

import com.iamxgw.mydb.backend.TransactionManager;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
    private long xidCounter;
    private Lock counterLock;

    public TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     *
     * */
    private void checkXIDCounter() {
        long fileLen = 0;
        try {
            fileLen = file.length();

        } catch (IOException e1) {
//            Panic
        }
    }

    @Override
    public long bagin() {
        return 0;
    }

    @Override
    public void commit(long xid) {

    }

    @Override
    public void abort(long xid) {

    }

    @Override
    public boolean isActivate(long xid) {
        return false;
    }

    @Override
    public boolean isCommitted(long xid) {
        return false;
    }

    @Override
    public boolean isAborted(long xid) {
        return false;
    }

    @Override
    public void close() {

    }
}
