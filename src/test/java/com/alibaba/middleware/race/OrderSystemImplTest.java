package com.alibaba.middleware.race;

import com.alibaba.middleware.race.datastruct.BplusTree;
import com.alibaba.middleware.race.process.IndexProcessor;
import com.alibaba.middleware.race.process.QueryProcessor;
import com.alibaba.middleware.race.util.Utils;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.channels.FileChannel;
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
        QueryProcessor queryProcessor = new QueryProcessor();
        System.out.println("Search useTime: " + (System.currentTimeMillis() - start));
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
