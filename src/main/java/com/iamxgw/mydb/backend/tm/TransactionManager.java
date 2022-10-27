package com.iamxgw.mydb.backend.tm;

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
}
