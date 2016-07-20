package com.alibaba.middleware.race;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

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
        File file = new File(orderFiles[0]);
//        BufferedReader bf = new BufferedReader(new FileReader(file));
//        String str = bf.readLine();
//        long start = System.currentTimeMillis();
////        int count =
//        while (str!=null) {
//            str = bf.readLine();
//        }
//        System.out.println(System.currentTimeMillis()-start);

        FileChannel fc = new FileInputStream(file).getChannel();
        long length = file.length();
        long start = System.currentTimeMillis();
        MappedByteBuffer in = fc.map(FileChannel.MapMode.READ_ONLY,10000000,1024);
        byte[] bytes = new byte[1024];
        ByteBuffer b1 = in.load();
        String str = String.valueOf(in.get(bytes,0,1024));
        System.out.println(System.currentTimeMillis()-start);

    }
}
