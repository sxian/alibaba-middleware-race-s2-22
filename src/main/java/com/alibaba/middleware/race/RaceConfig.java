package com.alibaba.middleware.race;

/**
 * Created by sxian.wang on 2016/7/19.
 */
public class RaceConfig {


    public static String STORE_PATH = "t/index/";
    public static String DATA_ROOT = "prerun_data/";
    public static String DISK1 = "t/d1/";
    public static String DISK2 = "t/d2/";
    public static String DISK3 = "t/d3/";

    // 保存排序完的订单数据

    public static int ORDER_FILE_SIZE = 61; // 线上200
    public static int BUYER_FILE_SIZE = 20; // 10
    public static int GOODS_FILE_SIZE = 20; // 10
    public static int HB_FILE_SIZE = 20;
    public static int HG_FILE_SIZE = 20;

}
