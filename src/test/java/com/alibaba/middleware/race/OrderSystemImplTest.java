package com.alibaba.middleware.race;

import com.alibaba.middleware.race.datastruct.Query;
import com.alibaba.middleware.race.db.OrderTable;
import com.alibaba.middleware.race.db.Table;
import com.alibaba.middleware.race.util.Utils;

import java.io.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.alibaba.middleware.race.RaceConfig.DATA_ROOT;

/**
 * Created by sxian.wang on 2016/7/19.
 */
public class OrderSystemImplTest {
    public static int querySum = 0;
    public static int successSum = 0;
    public static int failed = 0;
    private static final LinkedBlockingQueue<Query> queryQueue = new LinkedBlockingQueue<>(2000);
    private static final LinkedBlockingQueue<Query> resultQueue = new LinkedBlockingQueue<>(2000);
    public static OrderSystemImpl osi;

    public static void main(String[] args) throws IOException, InterruptedException {

        String[]  orderFiles = new String[]{DATA_ROOT+"order.0.0", DATA_ROOT+"order.0.3",
                DATA_ROOT+"order.1.1", DATA_ROOT+"order.2.2"};

        String[] buyerFiles = new String[]{DATA_ROOT+"buyer.0.0", DATA_ROOT+"buyer.1.1"};

        String[] goodsFiles = new String[]{DATA_ROOT+"good.0.0", DATA_ROOT+"good.1.1",
                DATA_ROOT+"good.2.2"};

        String[] storeFiles = new String[]{"t/index"};


        OrderSystemImpl orderSystem = new OrderSystemImpl();
        osi = orderSystem;
        List<String> orderList = Arrays.asList(orderFiles);
        List<String> buyerList = Arrays.asList(buyerFiles);
        List<String> goodsList = Arrays.asList(goodsFiles);
        List<String> storeList = Arrays.asList(storeFiles);

        long start = System.currentTimeMillis();
        orderSystem.construct(orderList, buyerList, goodsList, storeList);
        System.out.println("Build useTime: " + (System.currentTimeMillis() - start));
        Thread buildThread = new Thread(new BuildQuery());
        buildThread.start();
        Thread compareThread = new Thread(new CompareResult());
        compareThread.start();
//        Thread.sleep(10000);
        start = System.currentTimeMillis();
        while (true) {
            Query query = queryQueue.take();
            if (query.queryEnd) {
                resultQueue.offer(query);
                break;
            }
            querySum++;
            switch (query.queryFlag) {
                case 0:
                    query.result =  orderSystem.queryOrder(Long.valueOf(query.id),query.keys);
                    break;
                case 1:
                    query.result1 = orderSystem.queryOrdersByBuyer(query.start,query.end,query.id);

                    break;
                case 2:
                    query.result1 = orderSystem.queryOrdersBySaler("",query.id,query.keys);
                    break;
                case 3:
                    query.kv =  orderSystem.sumOrdersByGood(query.id,query.keys.get(0)); // 性能瓶颈
                    break;
            }
            resultQueue.offer(query,600,TimeUnit.SECONDS);
        }
        System.out.println("query number is: "+querySum+", success num is: "+successSum+", failed num is: " +failed);
        System.out.println("Query useTime: " + (System.currentTimeMillis() - start));
        buildThread.interrupt();
        compareThread.interrupt();
    }

    public static ArrayList<OrderSystemImpl.Row> buildQueryList(String[] files) throws IOException {
        ArrayList<OrderSystemImpl.Row> rows = new ArrayList<>();

        for (String file : files) {
            BufferedReader br = Utils.createReader(file);
            String str = br.readLine();
            while (str!=null) {
                OrderSystemImpl.Row row = OrderSystemImpl.createRow(str);
                rows.add(row);
                str = br.readLine();
            }
        }
        return rows;
    }

    public static HashMap<String,OrderSystemImpl.Row> buildQueryMap(Table table, List<OrderSystemImpl.Row> rows, String id) {
        HashMap<String,OrderSystemImpl.Row> map =new HashMap<>();
        for (int i = 0;i<rows.size();i++) {
            String key = rows.get(i).get(id).valueAsString();
            OrderSystemImpl.Row row = OrderSystemImpl.createRow(table.selectRowById(key));
            if (row!=null) {
                map.put(key,row);
            }
        }
        return map;
    }

    public static void testSelectOrderIDByBuyerID(String id, Random random, String strs,OrderTable table) {
        System.out.println("**************");
        TreeMap<String, String> map = new TreeMap<>();
        for (String str : strs.split(" ")) {
            String[] kv = str.split(",");
            map.put(kv[1],kv[0]); // 时间 订单号
        }
        List<String> keyList = new ArrayList<>(map.keySet());
        int s = random.nextInt(keyList.size()-1);
        int e = random.nextInt(keyList.size()-1);
        while (s==e || s>e) {
            if (s==e) {
                s = random.nextInt(keyList.size()-1);
                e = random.nextInt(keyList.size()-1);
            } else {
                int tmp = e;
                e = s;
                s = tmp;
            }
        }
        long start = Long.valueOf(keyList.get(s));
        long end = Long.valueOf(keyList.get(e));

        List<String> list = table.selectOrderIDByBuyerID(id,start,end);
        System.out.println("query result count: " + list.size()+", should: "+(e-s));
        for (int i = 0;i<e-s;i++) {
            System.out.println("------------");
            System.out.println("time is:"+keyList.get(s+i)+", result should: " +map.get(keyList.get(s+i))+", fact: "+
                    list.get(i)+ ". result is: "+map.get(keyList.get(s+i)).equals(list.get(i).split(",")[1]));
        }
    }
    public static void countSuccess(List<OrderSystemImpl.Row> rows, Map<String,OrderSystemImpl.Row> map,String id) {
        int find = 0;
        int matched = 0;
        for (int i = 0;i<rows.size();i++) {
            OrderSystemImpl.Row row = rows.get(i);
            OrderSystemImpl.Row _row = map.get(row.get(id).valueAsString());
            if (_row!=null) {
                find++;
                Set<Map.Entry<String, OrderSystemImpl.KV>> entrySet = row.entrySet();
                boolean ok = true;
                for (Map.Entry<String, OrderSystemImpl.KV> entry : entrySet) {
                    try {
                        OrderSystemImpl.KV kv = _row.getKV(entry.getKey());
                    } catch (Exception e) {
                        ok = false;
                        break;
                    }
                }
                if (ok) {
                    matched++;
                } else {
                    int a = 5;
                }
            }
        }
        System.out.println("find data: "+ find+"\nmatched data: "+matched);
    }

    private static boolean compareOrder(String id, HashMap<String,ArrayList<OrderSystemImpl.KV>> resultMap,
                                     OrderSystem.Result result) {
        String orderid = String.valueOf(result.orderId());

        boolean queryok = true;

        ArrayList<OrderSystemImpl.KV> list = resultMap.get(orderid);

        for (int i = 0;i<list.size();i++) {
            OrderSystemImpl.KV kv = list.get(i);
            OrderSystemImpl.KV kv1 = (OrderSystemImpl.KV) result.get(kv.key());
            if (kv1==null) {
                System.out.println("queryOrder kv is null. shoud: " +kv.valueAsString());
                queryok = false;
                continue;
            }
            if (!kv.valueAsString().equals(kv1.valueAsString())) {
                try {
                    if (kv.valueAsLong() == kv1.valueAsLong()) {
                        return queryok;
                    }
                } catch (OrderSystem.TypeException e) {
                    try {
                        if (kv.valueAsDouble() == kv1.valueAsDouble()) {
                            return queryok;
                        }
                    } catch (OrderSystem.TypeException e1) {
                    }
                }
                queryok = false;
                System.out.println("queryOrder kv error, should: " + kv.valueAsString() +
                        ", but is: "+result.get(kv.key()).valueAsString()+"***");
            }
        }
        return queryok;
    }

    static class BuildQuery implements Runnable {

        @Override
        public void run() {
            String filePath = DATA_ROOT+"case.0";
            BufferedReader br = null;
            try {
                br = Utils.createReader(filePath);
                boolean resultFlag = false;
                boolean queryed = false;
                Query query = new Query();
                String record = br.readLine();
                long start = System.currentTimeMillis();
                int queryNum = 0;
                while (record!=null) {
                    query.recordList.add(record);
                    if ((record.equals("}")||record.equals(""))) {
                        if (!queryed) {
                            if (queryNum++%1000 == 0) {
                                System.out.println("Already build query num: "+queryNum+", use time: "+
                                        (System.currentTimeMillis()-start)+"queue size: " + queryQueue.size());
                            }
                            queryQueue.offer(query,60, TimeUnit.SECONDS);
                            query = new Query();
                            resultFlag = false;
                            queryed = true;
                        }
                        record = br.readLine();
                        continue;
                    }

                    if (resultFlag) {
                        String[] strs = record.replace("{","").replace("}","").split(", KV:");
                        String key = strs[0].split(":")[1];
                        ArrayList<OrderSystemImpl.KV> list = new ArrayList<>();
                        for (String kvStr : strs[1].replace("[","").replace("]","").split(",")) {
                            String[] kv = kvStr.split(":");
                            if (kv.length <= 1) continue;

                            list.add(OrderSystemImpl.createKV(kv[0],kv[1]));
                        }
                        query.resultMap.put(key, list);
                        record = br.readLine();
                        continue;
                    }

                    String[] kv = record.split(":");
                    switch (kv[0]) {
                        case "CASE":
                            queryed = false;
                            switch (kv[1]) {
                                case "QUERY_ORDER":
                                    query.queryFlag = 0;
                                    break;
                                case "QUERY_BUYER_TSRANGE":
                                    query.queryFlag = 1;
                                    break;
                                case "QUERY_SALER_GOOD":
                                    query.queryFlag = 2;
                                    break;
                                case "QUERY_GOOD_SUM":
                                    query.queryFlag = 3;
                                    break;
                            }
                            break;
                        case "ORDERID":
                            query.id = kv[1];
                            break;
                        case "BUYERID":
                            query.id = kv[1];
                            break;
                        case "GOODID":
                            query.id = kv[1];
                            break;
                        case "KEYS":
                            String str = kv[1].replace("[","").replace("]","");
                            if (str.contains(",")) {
                                for (String k : str.split(",")) {
                                    if (k.equals("*")) {
                                        query.keys = null;
                                        break;
                                    }
                                    query.keys.add(k);
                                }
                            } else {
                                query.keys.add(str);
                            }
                            break;
                        case "STARTTIME":
                            query.start = Long.valueOf(kv[1]);
                            break;
                        case "ENDTIME":
                            query.end = Long.valueOf(kv[1]);
                            break;
                        case "Result":
                            resultFlag = true;
                            break;
                        case "RESULT":
                            if (kv[1].contains(".")) {
                                query.sumIsDoube = true;
                                try {
                                    query.resultDouble = Double.valueOf(kv[1]);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            } else {
                                try {
                                    query.resultLong = Long.valueOf(kv[1]);
                                } catch (Exception e) {
                                    query.sumIsNull = true;
                                }
                            }
                            break;
                    }
                    record = br.readLine();
                }

                System.out.println("build Test data use time: " +(System.currentTimeMillis()-start)+", build query num: "+queryNum);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                Query query = new Query();
                query.queryEnd = true;
                queryQueue.offer(query);
                try {
                    if (br!=null)
                        br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static class  CompareResult implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    Query query = resultQueue.take(); // 0 是查询结果 1是答案
                    if (query.queryEnd) break;
                    switch (query.queryFlag) {
                        case 0:
                            OrderSystem.Result result = query.result;
                            if (query.resultMap.size()==0) {
                                if (result==null) {
                                    successSum++;
                                } else {
                                    failed++;
                                    osi.queryOrder(Long.valueOf(query.id),query.keys);
                                    System.out.println("result is null, but exist data");
                                }
                                break;
                            }
                            if (!query.id.equals(String.valueOf(result.orderId()))) {
                                failed++;
                                osi.queryOrder(Long.valueOf(query.id),query.keys);
                            }
                            if (compareOrder(query.id,query.resultMap ,result)){
                                successSum++;
                            }else {
                                failed++;
                            }
                            break;
                        case 1:
                            Iterator<OrderSystem.Result> result1 = query.result1;
                            boolean queryok = true;
                            while (result1.hasNext()) {
                                if (!compareOrder(query.id,query.resultMap ,result1.next()) && queryok) {
                                    queryok = false;
                                }
                            }
                            if (queryok) successSum++;
                            else failed++;
                            break;
                        case 2:
                            Iterator<OrderSystem.Result> result2 =query.result1;
                            boolean queryok1 = true;
                            while (result2.hasNext()) {
                                if (!compareOrder(query.id,query.resultMap ,result2.next()) && queryok1) {
                                    queryok1 = false;
                                }
                            }
                            if (queryok1) successSum++;
                            break;
                        case 3:
                            OrderSystem.KeyValue kv = query.kv;
                            if (kv==null) {
                                if (query.sumIsNull) {
                                    successSum++;
                                } else {
                                    failed++;
//                                    System.out.println("sum error: " + query.id +", should: (Long) "+query.resultLong+" / (Double) "+query.resultDouble+", but is not contains key: "+ query.keys.get(0));
                                }
                                break;
                            }
                            String key = kv.key();
                            boolean queryok2 = true;
                            if (!query.keys.get(0).equals(key)) {
                                System.out.println("key no match: " + query.id +", should: "+query.keys.get(0)+", but is " + key);
                                queryok2 = false;
                            }
                            if (query.sumIsDoube) {
                                try {
                                    double res = kv.valueAsDouble();
                                    if (Math.abs(res - query.resultDouble) >= 0.0001) {
                                        System.out.println("sum error: " + query.id +", should: "+query.resultDouble+", but is " + res);
                                        queryok2 = false;
                                    }
                                } catch (OrderSystem.TypeException e) {
                                }
                            } else {
                                try {
                                    long res = kv.valueAsLong();
                                    if (res != query.resultLong) {
                                        System.out.println("sum error: " + query.id +", should: "+query.resultLong+", but is " + res);
                                        queryok2 = false;
                                    }
                                } catch (OrderSystem.TypeException e) {
                                }
                            }
                            if (queryok2) successSum++;
                            else failed++;
                            break;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

