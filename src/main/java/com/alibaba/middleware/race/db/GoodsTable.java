package com.alibaba.middleware.race.db;

import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.cache.LRUCache;
import com.alibaba.middleware.race.process.QueryProcessor;
import com.alibaba.middleware.race.util.Utils;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by sxian.wang on 2016/7/21.
 */
public class GoodsTable {
    private LRUCache<String, OrderSystemImpl.Row> rowCache;
    public final LinkedBlockingQueue<OrderSystemImpl.Row> syncQueue = new LinkedBlockingQueue<>(1000);

    public GoodsTable() {
        rowCache = new LRUCache<>(300000);
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        OrderSystemImpl.Row row = syncQueue.take();
                        rowCache.put(row.get("goodid").valueAsString(),row);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    public OrderSystemImpl.Row selectRowById(String id) {
        OrderSystemImpl.Row row = rowCache.get(id);
        if (row == null) {
            try {
                row = Utils.createRow(QueryProcessor.queryGoods(id));
                if (row!=null) syncQueue.offer(row, 30, TimeUnit.SECONDS);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return row;
    }
}
