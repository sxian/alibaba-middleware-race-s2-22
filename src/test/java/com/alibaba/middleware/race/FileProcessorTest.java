package com.alibaba.middleware.race;

import com.alibaba.middleware.race.process.QueryProcessor;
import com.alibaba.middleware.race.util.Utils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

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
        String str = "t\\index\\orderIndex_1";
        BufferedReader br = Utils.createReader(str);
        String line = br.readLine();
        Random random = new Random(47);
        long start = System.currentTimeMillis();

        while (line!=null) {
            String[] indexs = line.split("\t");
            String key = indexs[0];
            String path = indexs[1];
            long pos = Long.valueOf(indexs[2]);
            int length = Integer.valueOf(indexs[3]);
            String value = QueryProcessor.query(path,pos,length);
            for (String kv : value.split("\t")) {
                System.out.println(kv);
            }
            System.out.println("*******"+key+"******");
            System.out.println();
            line = br.readLine();
        }
        System.out.println(System.currentTimeMillis()-start);
    }
}
