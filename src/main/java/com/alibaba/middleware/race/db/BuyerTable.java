package com.alibaba.middleware.race.db;

import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.cache.LRUCache;
import com.alibaba.middleware.race.process.QueryProcessor;

import java.io.IOException;

/**
 * Created by sxian.wang on 2016/7/21.
 */
public class BuyerTable {
    private LRUCache<String, String> rowCache;

    public BuyerTable() {
        rowCache = new LRUCache<>(100000);
    }

    public String selectRowById(String id) {
        String row = rowCache.get(id);
        if (row == null) {
            try {
                row = QueryProcessor.queryBuyer(id);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (row!=null) rowCache.put(id, row);
        }
        return row;
    }
}
