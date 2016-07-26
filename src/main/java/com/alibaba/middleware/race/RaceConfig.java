package com.alibaba.middleware.race;

/**
 * Created by sxian.wang on 2016/7/19.
 */
public class RaceConfig {

    public static final boolean ONLINE = false;

    public static String LOCAL_STORE_PATH = "t/index/";
    public static String STORE_PATH = "t/index/";
    public static String DATA_ROOT = "prerun_data/";
    public static String DISK1 = "";
    public static String DISK2 = "";
    public static String DISK3 = "";

    // 保存排序完的订单数据
    public static String ORDER_SOTRED_STORE_PATH = "t/";

    public static int ORDER_FILE_SIZE = 5; // 线上200

    public static int BUYER_FILE_SIZE = 2; // 5

    public static int GOODS_FILE_SIZE = 2; // 5
    // 多少个文件对应一个数据队列，亦即一个线程处理多少个文件
    public static int CONSTRUCT_MOD_NUM = 2; // 5
}
