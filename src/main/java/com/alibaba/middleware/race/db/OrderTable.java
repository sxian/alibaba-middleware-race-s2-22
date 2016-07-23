package com.alibaba.middleware.race.db;

import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.OrderSystemImpl.Row;
import com.alibaba.middleware.race.cache.LRUCache;
import com.alibaba.middleware.race.process.QueryProcessor;

import java.io.IOException;

/**
 * Created by sxian.wang on 2016/7/21.
 */
public class OrderTable implements Table{
    private LRUCache<String, Row> rowCache; // todo 计算一个entry的大小

    public OrderTable() {
        rowCache = new LRUCache<>(100000);
    }

    @Override
    public Row selectRowById(String id) {
        Row row = rowCache.get(id);
        if (row == null) {
            row = QueryProcessor.queryOrder(id);
            if (row!=null) rowCache.put(id, row);
        }
        return row;
    }
}
