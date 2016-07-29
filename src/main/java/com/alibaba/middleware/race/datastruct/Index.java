package com.alibaba.middleware.race.datastruct;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by sxian.wang on 2016/7/29.
 */
public class Index {
    private static final int BUCKET_SIZE = 10000;

    private ArrayList<String>[] _list = new ArrayList[BUCKET_SIZE];

    public Index() {
        for (int i = 0;i<10000;i++) {
            _list[i] = new ArrayList<>();
        }
    }

    public void add(String key,String index) {
        _list[Math.abs(key.hashCode())].add(index);
    }
}
