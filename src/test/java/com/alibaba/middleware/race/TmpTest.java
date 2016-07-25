package com.alibaba.middleware.race;

import org.omg.SendingContext.RunTime;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by sxian.wang on 2016/7/23.
 */
public class TmpTest {
    public static float M = 1024 * 1024;
    public static void main(String[] args) throws IOException {
//        RandomAccessFile raf = new RandomAccessFile("t/index/goodid_orderid","r");
        RandomAccessFile raf = new RandomAccessFile("t/index/buyerid_create_order","r");

        String st1 = "";
//        List list = null;
//        for (String str : list) {
//
//        }

        raf.seek(7259995); // ap-804e-6f1c7d177abd,25468,470
        byte[] bytes = new byte[798];
        raf.read(bytes);
        System.out.println(new String(bytes));

//        String str = "+5648431684";
//        long strL = Long.valueOf(str);
//        System.out.println(strL);

////        String str = "orderid:592084336\tcreatetime:1470982941\tbuyerid:ap-a3a3-4dc3d77d2a79\tgoodid:dd-a6b7-d85a6829f798\tamount:17\tdone:true\ta_o_4699:8495\ta_o_7517:true\ta_o_15325:0.74\ta_o_22304:0.4\ta_o_12368:true";
////        String reg = "(orderid|buyerid|goodid|createtime):([\\w|-]+)";
////        Pattern pattern = Pattern.compile(reg);
////        Matcher matcher = pattern.matcher(str);
////        while (matcher.find()) {
////            System.out.println(matcher.group(0));
////            System.out.println(matcher.group(1));
////            System.out.println(matcher.group(2));
////            System.out.println("***********");
////        }
//
////        System.out.println(9/10);
//        Runtime runTime = Runtime.getRuntime();
//        int count = 0;
//        long start = System.currentTimeMillis();
//        System.out.println(runTime.freeMemory());
//        ArrayList<String> list = new ArrayList<>();
//        long[] longs = new long[100000000];
//        System.out.println(runTime.freeMemory());
//        Random random = new Random();
//        while (count<100000000) {
//            longs[count++]= random.nextLong();
//
//        }
//        System.out.println(runTime.freeMemory());
//        System.out.println("use time: "+ (System.currentTimeMillis() - start));
    }



}
