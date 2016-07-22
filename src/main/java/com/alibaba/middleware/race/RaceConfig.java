package com.alibaba.middleware.race;

/**
 * Created by sxian.wang on 2016/7/19.
 */
public class RaceConfig {

    public static final boolean ONLINE = false;

    public static final String DATA_ROOT = "prerun_data/";
    public static final String STORE_PATH = "t/";

    public static final int ORDER_FILE_SIZE = 3;
    public static final int ORDER_INDEX_FILE_SIZE = 1;

    public static final int BUYER_FILE_SIZE = 1;
    public static final int BUYER_INDEX_FILE_SIZE = 1;

    public static final int GOODS_FILE_SIZE = 1;
    public static final int GOODS_INDEX_FILE_SIZE  = 1;
    // 多少个文件对应一个数据队列，亦即一个线程处理多少个文件
    public static final int CONSTRUCT_MOD_NUM = 3;
}
