package com.alibaba.middleware.race;

import java.io.IOException;

/**
 * Created by sxian.wang on 2016/7/19.
 */
public class FileProcessorTest {
    public static final String[]  orderFiles = new String[]{RaceConfig.DATA_ROOT+"order.0.0", RaceConfig.DATA_ROOT+"order.0.3",
            RaceConfig.DATA_ROOT+"order.1.1", RaceConfig.DATA_ROOT+"order.2.2"};

    public static final String[] buyerFiles = new String[]{RaceConfig.DATA_ROOT+"buyer.0.0", RaceConfig.DATA_ROOT+"buyer.1.1"};

    public static final String[] goodsFiles = new String[]{RaceConfig.DATA_ROOT+"good.0.0", RaceConfig.DATA_ROOT+"good.1.1",
            RaceConfig.DATA_ROOT+"good.2.2"};

    public static final String caseFile = RaceConfig.DATA_ROOT+"case.0";

    public static void main(String[] args) throws IOException {
    }
}
