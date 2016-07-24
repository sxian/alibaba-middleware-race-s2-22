package com.alibaba.middleware.race;

import com.alibaba.middleware.race.db.OrderTable;
import com.alibaba.middleware.race.util.Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
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

        OrderTable orderTable = new OrderTable();
        HashMap<String,OrderSystemImpl.Row> map = new HashMap<>();
        ArrayList<OrderSystemImpl.Row> rows = new ArrayList<>();
        for (String file : orderFiles) {
            BufferedReader br = Utils.createReader(file);
            String str = br.readLine();
            while (str!=null) {
                OrderSystemImpl.Row row = OrderSystemImpl.createRow(str);
                rows.add(row);
                str = br.readLine();
            }
        }
        System.out.println("build test data complete, start query...");
        start = System.currentTimeMillis();
        for (int i = 0;i<rows.size();i++) {
            String key = rows.get(i).get("orderid").valueAsString();
            OrderSystemImpl.Row row = orderTable.selectRowById(key);
            if (row!=null) {
                map.put(key,row);
            }
        }
        System.out.println("query complete, useTime: "+(System.currentTimeMillis() - start) );
        System.out.println("start count success...");

        int find = 0;
        int matched = 0;
        for (int i = 0;i<rows.size();i++) {
            OrderSystemImpl.Row row = rows.get(i);
            OrderSystemImpl.Row _row = map.get(row.get("orderid").valueAsString());
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
