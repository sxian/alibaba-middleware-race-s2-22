package com.alibaba.middleware.race;

/**
 * Created by sxian.wang on 2016/7/19.
 */
public class RaceConfig {

    public static final boolean ONLINE = true;

    public static String ORDER_SOTRED_STORE_PATH = "t/index/";
    public static String STORE_PATH = "t/index/";
    public static String DATA_ROOT = "prerun_data/";
    public static String DISK1 = "t/d1/";
    public static String DISK2 = "t/d2/";
    public static String DISK3 = "t/d3/";

    // 保存排序完的订单数据

    public static int ORDER_FILE_SIZE = 1; // 线上200
    public static int BUYER_FILE_SIZE = 1; // 10
    public static int GOODS_FILE_SIZE = 1; // 10
    public static int BUYER_TO_ORDER_SIZE = 1;
    public static int GOODS_TO_ORDER_SIZE = 1;

}
