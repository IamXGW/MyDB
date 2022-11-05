package com.iamxgw.mydb.backend.tm;

import com.iamxgw.mydb.backend.utils.Panic;
import com.iamxgw.mydb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * TM 通过维护 XID 文件来维护事务的状态，并提供接口供其他模块来查询某个事务的状态
 */
public interface TransactionManager {
    // 开启新事务
    long begin();
    // 提交事务
    void commit(long xid);
    // 取消事务
    void abort(long xid);
    // 查询一个事务是不是正在进行的状态
    boolean isActivate(long xid);
    // 查询一个事务的状态是否已提交
    boolean isCommitted(long xid);
    // 查询一个事务的状态是否已取消
    boolean isAborted(long xid);
    // 关闭 TM TransactionManager
    void close();

    /**
     * 根据 path 创建一个 .xid 文件
     * @param path
     * @return
     */
    public static TransactionManagerImpl create(String path) {
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        try {
            if (!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        // 写空 XID 文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }

    /**
     * 打开位于 path 路径的 .XID 文件
     * @param path
     * @return
     */
    public static TransactionManagerImpl open(String path) {
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        if (!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if (!f.canWrite() || !f.canRead()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }
}
