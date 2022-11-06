package com.iamxgw.mydb.backend.dm.logger;

import com.iamxgw.mydb.backend.utils.Panic;
import com.iamxgw.mydb.backend.utils.Parser;
import com.iamxgw.mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import com.google.common.primitives.Bytes;

public class LoggerImpl implements Logger {
    private static final int SEED = 13331;

    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;
    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;

    // 当前日志指针位置，以一个日志为单位。每次都指在一条日志的开头
    private long position;
    // 初始化时确定，log 操作不更新
    // TODO: fileSize 为什么不用变？是因为只有在初始化和调用 next 方法时才会使用到 fileSize？
    private long fileSize;
    private int xChecksum;

    public LoggerImpl(RandomAccessFile file, FileChannel fc) {
        this.file = file;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    public LoggerImpl(RandomAccessFile file, FileChannel fc, int xChecksum) {
        this.file = file;
        this.fc = fc;
        this.xChecksum = xChecksum;
        this.lock = new ReentrantLock();
    }

    /**
     * 初始化方法，在 open 一个日志文件时会调用
     * 1. 确定 fileSize 大小
     * 2. 检查检验和并移除 badTail
     */
    void init() {
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }

        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position();
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xChecksum;
        
        checkAndRemoveTail();
    }

    /**
     * 检查并移除 badTail
     */
    private void checkAndRemoveTail() {
        rewind();

        int xChexk = 0;
        while (true) {
            byte[] log = internNext();
            if (log == null) break;
            xChexk = calChecksum(xChexk, log);
        }
        if (xChexk != xChecksum) {
            Panic.panic(Error.BadLogFileException);
        }

        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }

        rewind();
    }

    private byte[] internNext() {
        if (OF_DATA + position >= fileSize) {
            return null;
        }
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(tmp);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());
        if (OF_DATA + position + size > fileSize) {
            return null;
        }

        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        byte[] log = buf.array();
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        if (checkSum1 != checkSum2) {
            return null;
        }
        position += log.length;
        return log;
    }

    /**
     * 计算日志 log 的校验和
     * @param xCheck
     * @param log
     * @return
     */
    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    /**
     * 记录一条 log
     * @param data
     */
    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        updateXChecksum(log);
    }

    /**
     * 将 log 的校验和更新到全局校验和 xChecksum 中
     * @param log
     */
    private void updateXChecksum(byte[] log) {
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 包装一条 log
     * [size] [Checksum] [Data]
     * @param data
     * @return
     */
    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, checksum, data);
    }

    /**
     * 截断文件到正常文件的末尾
     * @param x
     * @throws Exception
     */
    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    /**
     * log 的迭代器
     * @return
     */
    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if (log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 将 positon 指针放在文件头的 xChecksum 之后
     */
    @Override
    public void rewind() {
        position = 4;
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
