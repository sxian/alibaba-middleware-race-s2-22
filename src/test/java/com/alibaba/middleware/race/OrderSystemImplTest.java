package com.alibaba.middleware.race;

import com.alibaba.middleware.race.db.OrderTable;
import com.alibaba.middleware.race.db.Table;
import com.alibaba.middleware.race.util.Utils;

import java.io.*;
import java.util.*;

import static com.alibaba.middleware.race.RaceConfig.DATA_ROOT;

/**
 * Created by sxian.wang on 2016/7/19.
 */
public class OrderSystemImplTest {

    public static void main(String[] args) throws IOException, InterruptedException {

        String[]  orderFiles = new String[]{DATA_ROOT+"order.0.0", DATA_ROOT+"order.0.3",
                DATA_ROOT+"order.1.1", DATA_ROOT+"order.2.2"};

        String[] buyerFiles = new String[]{DATA_ROOT+"buyer.0.0", DATA_ROOT+"buyer.1.1"};

        String[] goodsFiles = new String[]{DATA_ROOT+"good.0.0", DATA_ROOT+"good.1.1",
                DATA_ROOT+"good.2.2"};

        String[] storeFiles = new String[]{"t/index"};

        String caseFile = DATA_ROOT+"case.0";

        OrderSystemImpl orderSystem = new OrderSystemImpl();

        List<String> orderList = Arrays.asList(orderFiles);
        List<String> buyerList = Arrays.asList(buyerFiles);
        List<String> goodsList = Arrays.asList(goodsFiles);
        List<String> storeList = Arrays.asList(storeFiles);

        long start = System.currentTimeMillis();
        orderSystem.construct(orderList, buyerList, goodsList, storeList);
        System.out.println("Build useTime: " + (System.currentTimeMillis() - start));
        start = System.currentTimeMillis();
        testCase(caseFile,orderSystem);

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
    public static void testCase(String filePath, OrderSystemImpl osi) throws IOException {
        BufferedReader br = Utils.createReader(filePath);
        String record = br.readLine();
        int queryFlag = 0; // 0,QUERY_ORDER 1,QUERY_BUYER_TSRANGE  2,QUERY_SALER_GOOD  3,QUERY_GOOD_SUM
        boolean resultFlag = false;
        boolean queryed = false;
        boolean sumIsDoube = false;
        String id = "";
        ArrayList<String> keys = new ArrayList<>();
        long start,end;
        start=end=0;
        HashMap<String,ArrayList<OrderSystemImpl.KV>> resultMap = new HashMap<>();

        long resultLong = 0;
        double resultDouble = 0.0;
        while (record!=null) {
            if ((record.equals("}")||record.equals(""))) { // todo 执行查询
                if (!queryed) {
                    switch (queryFlag) {
                        case 0:
                            OrderSystem.Result result = osi.queryOrder(Long.valueOf(id),keys);
                            compareOrder(id,resultMap ,result);
                            break;
                        case 1:
                            Iterator<OrderSystem.Result> result1 = osi.queryOrdersByBuyer(start,end,id);
                            while (result1.hasNext()) {
                                compareOrder(id,resultMap ,result1.next());
                            }
                            break;
                        case 2:
                            Iterator<OrderSystem.Result> result2 = osi.queryOrdersBySaler("",id,keys);
                            while (result2.hasNext()) {
                                compareOrder(id,resultMap ,result2.next());
                            }
                            break;
                        case 3:
                            OrderSystem.KeyValue kv = osi.sumOrdersByGood(id,keys.get(0));
                            String key = kv.key();
                            if (!keys.get(0).equals(key)) {
                                System.out.println("key no match: " + id +", should: "+keys.get(0)+", but is " + key);
                            }
                            if (sumIsDoube) {
                                try {
                                    double res = kv.valueAsDouble();
                                    if (res != resultDouble) {
                                        System.out.println("sum error: " + id +", should: "+resultDouble+", but is " + res);
                                    }
                                } catch (OrderSystem.TypeException e) {
                                    e.printStackTrace();
                                }
                            } else {
                                try {
                                    long res = kv.valueAsLong();
                                    if (res != resultLong) {
                                        System.out.println("sum error: " + id +", should: "+resultLong+", but is " + res);
                                    }
                                } catch (OrderSystem.TypeException e) {
                                    e.printStackTrace();
                                }

                            }
                            break;
                    }
                    queryed = true;
                }
                record = br.readLine();
                continue;
            }

            if (resultFlag) {
                String str = record.replace("{","").replace("}","");
                String[] strs = record.replace("{","").replace("}","").split(", ");
                String key = strs[0].split(":")[1];
                ArrayList<OrderSystemImpl.KV> list = new ArrayList<>();
                for (String kvStr : strs[1].replace("[","").replace("]","").split(",")) {
                    String[] kv = kvStr.split(":");
                    list.add(OrderSystemImpl.createKV(kv[0],kv[1]));
                }
                resultMap.put(key, list);
                record = br.readLine();
                continue;
            }

            String[] kv = record.split(":");
            switch (kv[0]) {
                case "CASE":
                    queryed = false;
                    switch (kv[1]) {
                        case "QUERY_ORDER":
                            queryFlag = 0;
                            break;
                        case "QUERY_BUYER_TSRANGE":
                            queryFlag = 1;
                            break;
                        case "QUERY_SALER_GOOD":
                            queryFlag = 2;
                            break;
                        case "QUERY_GOOD_SUM":
                            queryFlag = 3;
                            break;
                    }
                    break;
                case "ORDERID":
                    id = kv[1];
                    break;
                case "BUYERID":
                    id = kv[1];
                    break;
                case "GOODID":
                    id = kv[1];
                    break;
                case "KEYS":
                    String str = kv[1].replace("[","").replace("]","");
                    if (str.contains(",")) {
                        for (String k : str.split(",")) {
                            keys.add(k);
                        }
                    } else {
                        keys.add(str);
                    }
                    break;
                case "STARTTIME":
                    start = Long.valueOf(kv[1]);
                    break;
                case "ENDTIME":
                    end = Long.valueOf(kv[1]);
                    break;
                case "Result":
                    resultFlag = true;
                    if (queryFlag == 3) {
                        if (kv[1].contains(".")) {
                            sumIsDoube = true;
                            resultDouble = Double.valueOf(kv[1]);
                        } else {
                            resultLong = Long.valueOf(kv[1]);
                        }
                    }
                    break;
            }
            record = br.readLine();
        }
        br.close();
    }

    private static void compareOrder(String id, HashMap<String,ArrayList<OrderSystemImpl.KV>> resultMap,
                                     OrderSystem.Result result) {
        String orderid = result.get("orderid").valueAsString();
        if (!id.equals(orderid)) {
            System.out.println("queryOrder error, should: " + id+", but is: " + orderid);

            return;
        }
        ArrayList<OrderSystemImpl.KV> list = resultMap.get(orderid);
        for (int i = 0;i<list.size();i++) {
            OrderSystemImpl.KV kv = list.get(i);
            if (kv.valueAsString().equals(result.get(kv.key()).valueAsString())) {
                System.out.println("queryOrder kv error, should: " + kv.valueAsString() +
                        ", but is: "+result.get(kv.key()).valueAsString()+"***");
            }
        }
    }

}
