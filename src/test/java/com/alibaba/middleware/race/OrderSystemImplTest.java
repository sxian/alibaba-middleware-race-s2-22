package com.alibaba.middleware.race;

import com.alibaba.middleware.race.db.BuyerTable;
import com.alibaba.middleware.race.db.GoodsTable;
import com.alibaba.middleware.race.db.OrderTable;
import com.alibaba.middleware.race.db.Table;
import com.alibaba.middleware.race.util.Utils;

import java.io.*;
import java.util.*;

/**
 * Created by sxian.wang on 2016/7/19.
 */
public class OrderSystemImplTest {

    public static void main(String[] args) throws IOException, InterruptedException {

        String[]  orderFiles = new String[]{RaceConfig.DATA_ROOT+"order.0.0", RaceConfig.DATA_ROOT+"order.0.3",
                RaceConfig.DATA_ROOT+"order.1.1", RaceConfig.DATA_ROOT+"order.2.2"};

        String[] buyerFiles = new String[]{RaceConfig.DATA_ROOT+"buyer.0.0", RaceConfig.DATA_ROOT+"buyer.1.1"};

        String[] goodsFiles = new String[]{RaceConfig.DATA_ROOT+"good.0.0", RaceConfig.DATA_ROOT+"good.1.1",
                RaceConfig.DATA_ROOT+"good.2.2"};

        String[] storeFiles = new String[]{RaceConfig.STORE_PATH};

        String caseFile = RaceConfig.DATA_ROOT+"case.0";

        OrderSystemImpl orderSystem = new OrderSystemImpl();

        List<String> orderList = Arrays.asList(orderFiles);
        List<String> buyerList = Arrays.asList(buyerFiles);
        List<String> goodsList = Arrays.asList(goodsFiles);
        List<String> storeList = Arrays.asList(storeFiles);

        long start = System.currentTimeMillis();
        orderSystem.construct(orderList, buyerList, goodsList, storeList);
        System.out.println("Build useTime: " + (System.currentTimeMillis() - start));
//        OrderTable orderTable = new OrderTable();
//        BuyerTable buyerTable = new BuyerTable();
//        GoodsTable goodsTable = new GoodsTable();
//
//        ArrayList<OrderSystemImpl.Row> orderRows = buildQueryList(orderFiles);
//        System.out.println("build order data complete, start query...");
//        ArrayList<OrderSystemImpl.Row> buyerRows = buildQueryList(buyerFiles);
//        System.out.println("build buyer data complete, start query...");
//        ArrayList<OrderSystemImpl.Row> goodsRows = buildQueryList(goodsFiles);
//        System.out.println("build goods data complete, start query...");
//
//        start = System.currentTimeMillis();
//        HashMap<String,OrderSystemImpl.Row> orderMap = buildQueryMap(orderTable,orderRows,"orderid");
//        System.out.println("query order complete, useTime: "+(System.currentTimeMillis() - start) );
//        System.out.println("start count order success...");
//        countSuccess(orderRows, orderMap,"orderid");
//        System.out.println("****************************************");
//
//        start = System.currentTimeMillis();
//        HashMap<String,OrderSystemImpl.Row> buyerMap = buildQueryMap(buyerTable,buyerRows,"buyerid");
//        System.out.println("query buyer complete, useTime: "+(System.currentTimeMillis() - start) );
//        System.out.println("start count buyer success...");
//        countSuccess(buyerRows, buyerMap,"buyerid");
//        System.out.println("****************************************");
//
//        start = System.currentTimeMillis();
//        HashMap<String,OrderSystemImpl.Row> goodsMap = buildQueryMap(goodsTable,goodsRows,"goodid");
//        System.out.println("query goods complete, useTime: "+(System.currentTimeMillis() - start) );
//        System.out.println("start count goods success...");
//        countSuccess(goodsRows, goodsMap,"goodid");
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
            OrderSystemImpl.Row row = table.selectRowById(key);
            if (row!=null) {
                map.put(key,row);
            }
        }
        return map;
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
    public void ProcessCase(String filePath) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(new File(RaceConfig.DATA_ROOT+"order.2.2")));
        String record = br.readLine();
        while (record!=null) {
            if ((record.equals("}")||record.equals(""))) { // todo 执行查询
                record = br.readLine();
                continue;
            }

            String[] kv = record.split(":");
            switch (kv[0]) {
                case "CASE":

                    break;
                case "ORDERID":
                    break;
                case "SALERID":
                    break;
                case "GOODID":
                    break;
                case "KEYS":
                    break;
                case "STARTTIME":
                    break;
                case "ENDTIME":
                    break;
                case "Result":
                    break;
            }
            record = br.readLine();
        }
        br.close();
    }

}
