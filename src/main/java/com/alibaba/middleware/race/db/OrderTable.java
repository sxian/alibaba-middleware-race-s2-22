package com.alibaba.middleware.race.db;

import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.cache.LRUCache;
import com.alibaba.middleware.race.process.QueryProcessor;

import java.io.IOException;
import java.util.List;

/**
 * Created by sxian.wang on 2016/7/21.
 */
public class OrderTable {
    public LRUCache<String, String> rowCache; // todo 计算一个entry的大小

    public OrderTable() {
        rowCache = new LRUCache<>(400000);
    }

    public String selectRowById(String id) {
        String row = rowCache.get(id);
        if (row == null) {
            try {
                row = QueryProcessor.queryOrder(id);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (row!=null) rowCache.put(id, row);
        }
        return row;
    }


    public List<String> selectOrderIDByBuyerID(String buyerid, long start, long end) {
        try {
            return QueryProcessor.queryOrderidsByBuyerid(buyerid, start, end);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<String> selectOrderIDByGoodsID(String goodid) {
        try {
            return QueryProcessor.queryOrderidsByGoodsid(goodid);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
