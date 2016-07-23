package com.alibaba.middleware.race.db;

import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.cache.LRUCache;
import com.alibaba.middleware.race.process.QueryProcessor;

/**
 * Created by sxian.wang on 2016/7/21.
 */
public class BuyerTable implements Table {
    private LRUCache<String, OrderSystemImpl.Row> rowCache; // todo 计算一个entry的大小

    public BuyerTable() {
        rowCache = new LRUCache<>(100000);
    }

    @Override
    public OrderSystemImpl.Row selectRowById(String id) {
        OrderSystemImpl.Row row = rowCache.get(id);
        if (row == null) {
            row = QueryProcessor.queryBuyer(id);
            if (row!=null) rowCache.put(id, row);
        }
        return row;
    }
}
