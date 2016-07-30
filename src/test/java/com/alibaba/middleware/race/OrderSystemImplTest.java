package com.alibaba.middleware.race;

import com.alibaba.middleware.race.datastruct.Query;
import com.alibaba.middleware.race.db.OrderTable;
import com.alibaba.middleware.race.util.Utils;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.middleware.race.RaceConfig.DATA_ROOT;

/**
 * Created by sxian.wang on 2016/7/19.
 */
public class OrderSystemImplTest {
    public static AtomicInteger querySum = new AtomicInteger(0);
    public static AtomicInteger successSum = new AtomicInteger(0);
    public static AtomicInteger failed = new AtomicInteger(0);
    public static AtomicInteger queryNum = new AtomicInteger(0);
    public static AtomicInteger buildCaseThreadCount = new AtomicInteger(0);
    public static AtomicInteger compareCaseThreadCount = new AtomicInteger(0);
    public static AtomicInteger executeQueryThreadCount = new AtomicInteger(0);
    public static long buildStart;
    public static int spiltCaseFileNum = 10;
    public static CountDownLatch queryLatch = new CountDownLatch(spiltCaseFileNum);
    public static CountDownLatch compareLatch = new CountDownLatch(spiltCaseFileNum);

    public static String CASE_ROOT = RaceConfig.STORE_PATH+"case/";
    private static final LinkedBlockingQueue<Query>[] queryQueues = new LinkedBlockingQueue[spiltCaseFileNum];
    private static final LinkedBlockingQueue<Query>[] resultQueues = new LinkedBlockingQueue[spiltCaseFileNum];

    public static OrderSystemImpl osi;

    public static void main(String[] args) throws IOException, InterruptedException {

        String[]  orderFiles = new String[]{DATA_ROOT+"order.0.0", DATA_ROOT+"order.0.3",
                DATA_ROOT+"order.1.1", DATA_ROOT+"order.2.2"};

        String[] buyerFiles = new String[]{DATA_ROOT+"buyer.0.0", DATA_ROOT+"buyer.1.1"};

        String[] goodsFiles = new String[]{DATA_ROOT+"good.0.0", DATA_ROOT+"good.1.1",
                DATA_ROOT+"good.2.2"};

        String[] storeFiles = new String[]{"t/d1/","t/d2/","t/d3/"};

        osi = new OrderSystemImpl();
        List<String> orderList = Arrays.asList(orderFiles);
        List<String> buyerList = Arrays.asList(buyerFiles);
        List<String> goodsList = Arrays.asList(goodsFiles);
        List<String> storeList = Arrays.asList(storeFiles);

        long start = System.currentTimeMillis();
        osi.construct(orderList, buyerList, goodsList, storeList);
        System.out.println("Build useTime: " + (System.currentTimeMillis() - start));
        Utils.spilitCaseFile(DATA_ROOT+"case.0", CASE_ROOT, spiltCaseFileNum);
        start = System.currentTimeMillis();
        buildStart = start;
        for (int i = 0;i<spiltCaseFileNum;i++) {
            queryQueues[i] = new LinkedBlockingQueue<>(1000);
            resultQueues[i] = new LinkedBlockingQueue<>(1000);
            new Thread(new BuildQuery()).start();
            new Thread(new ExecuteQuery()).start();
            new Thread(new CompareResult()).start();
        }
        queryLatch.await();
        System.out.println("Query useTime: " + (System.currentTimeMillis() - start));
        compareLatch.await();
        System.out.println("query number is: "+querySum.get()+", success num is: "+successSum.get()+
                ", failed num is: " +failed.get());
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
            int index = buildCaseThreadCount.getAndAdd(1);
            String filePath = CASE_ROOT+index;
            LinkedBlockingQueue<Query> queryQueue = queryQueues[index];
            BufferedReader br = null;
            try {
                br = Utils.createReader(filePath);
                boolean resultFlag = false;
                boolean queryed = false;
                Query query = new Query();
                String record = br.readLine();
                while (record!=null) {
                    query.recordList.add(record);
                    if ((record.equals("}")||record.equals(""))) {
                        if (!queryed) {
                            if (queryNum.getAndAdd(1)%1000 == 0) {
                                System.out.println("Already build query num: "+queryNum+", use time: "+
                                        (System.currentTimeMillis()-buildStart)+", queue size: " + queryQueue.size());
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
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                System.out.println("build Test data use time: " +(System.currentTimeMillis()-buildStart)+", build query num: "+queryNum);
                Query query = new Query();
                query.queryEnd = true;
                try {
                    queryQueue.offer(query,60,TimeUnit.SECONDS);
                    if (br!=null)
                        br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static class ExecuteQuery implements Runnable {
        @Override
        public void run() {
            int index = executeQueryThreadCount.getAndAdd(1);
            LinkedBlockingQueue<Query> queryQueue = queryQueues[index];
            LinkedBlockingQueue<Query> resultQueue = resultQueues[index];
            try {
                while (true) {
                    Query query = queryQueue.take();
                    if (query.queryEnd) {
                        resultQueue.offer(query);
                        break;
                    }
                    switch (query.queryFlag) {
                        case 0:
                            querySum.getAndAdd(1);
                            query.result =  osi.queryOrder(Long.valueOf(query.id),query.keys);
                            resultQueue.offer(query,600,TimeUnit.SECONDS);

                            break;
                        case 1:
//                            query.result1 = osi.queryOrdersByBuyer(query.start,query.end,query.id);
//                            resultQueue.offer(query,600,TimeUnit.SECONDS);

                            break;
                        case 2:
//                            query.result1 = osi.queryOrdersBySaler("",query.id,query.keys);
                            break;
                        case 3:
//                            query.kv =  osi.sumOrdersByGood(query.id,query.keys.get(0)); // 性能瓶颈
                            break;
                    }
//                    resultQueue.offer(query,600,TimeUnit.SECONDS);
                }
            }catch (Exception e) {
                e.printStackTrace();
            } finally {
                queryLatch.countDown();
            }
        }
    }

    static class  CompareResult implements Runnable {
        @Override
        public void run() {
            int index = compareCaseThreadCount.getAndAdd(1);
            LinkedBlockingQueue<Query> resultQueue = resultQueues[index];
            try {
                while (true) {
                    Query query = resultQueue.take(); // 0 是查询结果 1是答案
                    if (query.queryEnd) break;
                    switch (query.queryFlag) {
                        case 0:
                            OrderSystem.Result result = query.result;
                            if (query.resultMap.size()==0) {
                                if (result==null) {
                                    successSum.getAndAdd(1);
                                } else {
                                    failed.getAndAdd(1);
                                    osi.queryOrder(Long.valueOf(query.id),query.keys);
                                    System.out.println("result is null, but exist data");
                                }
                                break;
                            }
                            try {
                            if (!query.id.equals(String.valueOf(result.orderId()))) {
                                failed.getAndAdd(1);
                                osi.queryOrder(Long.valueOf(query.id),query.keys);
                            }

                            } catch (Exception e ){
                                osi.queryOrder(Long.valueOf(query.id),query.keys);

                                int i = 1;
                            }
                            if (compareOrder(query.id,query.resultMap ,result)){
                                successSum.getAndAdd(1);
                            }else {
                                failed.getAndAdd(1);
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
                            if (queryok) successSum.getAndAdd(1);
                            else failed.getAndAdd(1);
                            break;
                        case 2:
                            Iterator<OrderSystem.Result> result2 =query.result1;
                            boolean queryok1 = true;
                            while (result2.hasNext()) {
                                if (!compareOrder(query.id,query.resultMap ,result2.next()) && queryok1) {
                                    queryok1 = false;
                                }
                            }
                            if (queryok1) successSum.getAndAdd(1);
                            break;
                        case 3:
                            OrderSystem.KeyValue kv = query.kv;
                            if (kv==null) {
                                if (query.sumIsNull) {
                                    successSum.getAndAdd(1);
                                } else {
                                    failed.getAndAdd(1);
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
                            if (queryok2) successSum.getAndAdd(1);
                            else failed.getAndAdd(1);
                            break;
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                compareLatch.countDown();
            }
        }
    }
}

