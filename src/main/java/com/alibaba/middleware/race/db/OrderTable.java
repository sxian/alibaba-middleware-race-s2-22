package com.alibaba.middleware.race.db;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.cache.LRUCache;
import com.alibaba.middleware.race.process.QueryProcessor;
import com.alibaba.middleware.race.util.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by sxian.wang on 2016/7/21.
 */
public class OrderTable {
    public LRUCache<String, OrderSystemImpl.Row> rowCache; // todo 计算一个entry的大小
    public final LinkedBlockingQueue<OrderSystemImpl.Row> syncQueue = new LinkedBlockingQueue<>();
    public OrderTable() {
        rowCache = new LRUCache<>(400000);
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        OrderSystemImpl.Row row = syncQueue.take();
                        rowCache.put(row.get("orderid").valueAsString(),row); // todo 做成异步的
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
                row = Utils.createRow(QueryProcessor.queryOrder(id));
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (row!=null) syncQueue.offer(row);
        }
        return row;
    }

    public List<OrderSystemImpl.Row> selectOrderIDByBuyerID(String buyerid, long start, long end) {
        ArrayList<OrderSystemImpl.Row> result = new ArrayList<>();
        try {
            List<String> orders = QueryProcessor.queryOrderidsByBuyerid(buyerid, start, end);
            for (int i = 0;i<orders.size();i++) {
                String orderid = orders.get(i);
                if (rowCache.get(orderid) == null) {
                    OrderSystemImpl.Row _row = selectRowById(orderid.substring(orderid.indexOf(",")+1));
                    syncQueue.offer(_row);
                    result.add(_row);
                } else {
                    result.add(rowCache.get(orderid));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        Collections.sort(result, new Comparator<OrderSystemImpl.Row>() {
            @Override
            public int compare(OrderSystemImpl.Row o1, OrderSystemImpl.Row o2) {
                try {
                    long time1 = o1.get("createtime").valueAsLong();
                    long time2 = o2.get("createtime").valueAsLong();
                    return time1 > time2 ? -1 : (time1 < time2 ? 1 : 0);
                } catch (OrderSystem.TypeException e) {
                    e.printStackTrace();
                }
                return 0;
            }
        });
        return result;
    }

    public List<OrderSystemImpl.Row> selectOrderIDByGoodsID(String goodid) {
        ArrayList<OrderSystemImpl.Row> result = new ArrayList<>();
        try {
            List<String> orders = QueryProcessor.queryOrderidsByGoodsid(goodid);
            List<String> todoQuery = new ArrayList<>();
            for (int i = 0;i<orders.size();i++) {
                String orderid = orders.get(i);
                if (rowCache.get(orderid) == null) {
                    todoQuery.add(orderid);
                } else {
                    result.add(rowCache.get(orderid));
                }
            }
            if (todoQuery.size()>0) {
                List<String> _result = QueryProcessor.batchQuery(todoQuery);
                for (int i = 0;i<_result.size();i++) {
                    OrderSystemImpl.Row row = Utils.createRow(_result.get(i));
                    syncQueue.offer(row);
                    result.add(row);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        Collections.sort(result, new Comparator<OrderSystemImpl.Row>() {
            @Override
            public int compare(OrderSystemImpl.Row o1, OrderSystemImpl.Row o2) {
                try {
                    long id1 = o1.get("orderid").valueAsLong();
                    long id2 = o2.get("orderid").valueAsLong();
                    return id1 > id2 ? 1 : (id1 < id2 ? -1 : 0);
                } catch (OrderSystem.TypeException e) {
                    e.printStackTrace();
                }
                return 0;
            }
        });
        return result;
    }
}
