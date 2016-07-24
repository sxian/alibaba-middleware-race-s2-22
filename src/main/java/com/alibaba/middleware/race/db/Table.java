package com.alibaba.middleware.race.db;

import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.cache.LRUCache;

/**
 * Created by sxian.wang on 2016/7/21.
 */
public interface Table {
    LRUCache<String, OrderSystemImpl.Row> rowCache = null;
    String selectRowById(String id);
}
