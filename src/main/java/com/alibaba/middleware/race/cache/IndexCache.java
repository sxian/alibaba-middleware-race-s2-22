package com.alibaba.middleware.race.cache;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by sxian.wang on 2016/8/2.
 */
public class IndexCache {
    ConcurrentHashMap<String,int[]> cache = new ConcurrentHashMap<>();
    private int capacity;
    public IndexCache(int capacity) {
        this.capacity = capacity;
    }

    public void put(String indexs) {
        if (cache.size() >= capacity) {
            return;
        }
        int start = 0;
        int split = indexs.indexOf(" ");
        while (split!=-1) {
            String index = indexs.substring(start,split);
            int first = index.indexOf(",");
            int last = index.lastIndexOf(",");
            String key = index.substring(0,first);
            if (!cache.contains(key)) {
                cache.put(key,new int[]{Integer.valueOf(index.substring(first+1,last)),
                        Integer.valueOf(index.substring(last+1))});
            }
            start = split+1;
            split = index.indexOf(" ", start);
        }
    }

    public int[] get(String key) {
        int[] index = cache.get(key);
        if (index!=null) {
            cache.remove(key);
        }
        return index;
    }
}
