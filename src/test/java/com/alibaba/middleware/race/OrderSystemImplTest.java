package com.alibaba.middleware.race;

import java.io.*;
import java.util.List;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

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
//        Thread.currentThread().interrupt();

        OrderSystemImpl orderSystem = new OrderSystemImpl();

        List<String> orderList = Arrays.asList(orderFiles);
        List<String> buyerList = Arrays.asList(buyerFiles);
        List<String> goodsList = Arrays.asList(goodsFiles);
        List<String> storeList = Arrays.asList(storeFiles);

        long start = System.currentTimeMillis();
        orderSystem.construct(orderList, buyerList, goodsList, storeList);
        System.out.println("useTime: " + (System.currentTimeMillis() - start));
    }
}
