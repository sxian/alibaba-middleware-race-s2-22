package com.alibaba.middleware.race.db;

import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.cache.LRUCache;
import com.alibaba.middleware.race.process.QueryProcessor;

import java.util.List;

/**
 * Created by sxian.wang on 2016/7/21.
 */
public class OrderTable implements Table {
    public LRUCache<String, String> rowCache; // todo 计算一个entry的大小

    public OrderTable() {
        rowCache = new LRUCache<>(400000);
    }

    public OrderSystemImpl.Row selectRowById(String id) {
        String row = rowCache.get(id);
        if (row == null) {
            row = QueryProcessor.queryOrder(id);
            if (row!=null) rowCache.put(id, row);
        }
        return OrderSystemImpl.createRow(row);
    }


    public List<String> selectOrderIDByBuyerID(String buyerid, long start, long end) {
        return QueryProcessor.queryOrderidsByBuyerid(buyerid, start, end);
    }

    public List<String> selectOrderIDByGoodsID(String goodid) {
        return QueryProcessor.queryOrderidsByGoodsid(goodid);
    }
}
