package com.alibaba.middleware.race.datastruct;

/**
 * Created by sxian.wang on 2016/7/19.
 */
public interface BTree {
    public RecordIndex get(Comparable key);   //查询

    public void remove(Comparable key);    //移除

    public void insertOrUpdate(Comparable key, RecordIndex obj);
}
