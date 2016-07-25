package com.alibaba.middleware.race.datastruct;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.OrderSystemImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by sxian.wang on 2016/7/25.
 */
public class Query {
    public int queryFlag = 0;// 0,QUERY_ORDER 1,QUERY_BUYER_TSRANGE  2,QUERY_SALER_GOOD  3,QUERY_GOOD_SUM
    public String id = "";
    public boolean sumIsDoube = false;
    public boolean sumIsNull = false;
    public long resultLong = 0;
    public double resultDouble = 0.0;
    public ArrayList<String> keys = new ArrayList<>();
    public long start = 0, end = 0;
    public HashMap<String,ArrayList<OrderSystemImpl.KV>> resultMap = new HashMap<>();

    public ArrayList<String> recordList = new ArrayList<>();
    public OrderSystem.Result result;
    public Iterator<OrderSystem.Result> result1;
    public OrderSystem.KeyValue kv;

    public boolean queryEnd = false;
}
