package com.alibaba.middleware.race;

/**
 * Created by sxian.wang on 2016/7/19.
 */
public class RaceConfig {

    public static boolean ONLINE = false;

    public static String DATA_ROOT = "prerun_data/";
    public static String STORE_PATH = "t/";
    // 保存排序完的订单数据
    public static String ORDER_SOTRED_STORE_PATH = "t/";

    public static int ORDER_FILE_SIZE = 1;
    public static int ORDER_INDEX_FILE_SIZE = 1;

    public static int BUYER_FILE_SIZE = 1;
    public static int BUYER_INDEX_FILE_SIZE = 1;

    public static int GOODS_FILE_SIZE = 1;
    public static int GOODS_INDEX_FILE_SIZE  = 1;
    // 多少个文件对应一个数据队列，亦即一个线程处理多少个文件
    public static int CONSTRUCT_MOD_NUM = 3;
}
