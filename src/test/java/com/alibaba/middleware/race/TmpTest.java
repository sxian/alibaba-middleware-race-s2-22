package com.alibaba.middleware.race;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by sxian.wang on 2016/7/23.
 */
public class TmpTest {
    public static void main(String[] args) throws IOException {
        RandomAccessFile raf = new RandomAccessFile("t/buyerid_create_order","r");
        String st1 = "";

        raf.seek(5475486); // ap-804e-6f1c7d177abd,25468,470
        byte[] bytes = new byte[4284];
        raf.read(bytes);
        System.out.println(new String(bytes));

//        String str = "orderid:592084336\tcreatetime:1470982941\tbuyerid:ap-a3a3-4dc3d77d2a79\tgoodid:dd-a6b7-d85a6829f798\tamount:17\tdone:true\ta_o_4699:8495\ta_o_7517:true\ta_o_15325:0.74\ta_o_22304:0.4\ta_o_12368:true";
//        String reg = "(orderid|buyerid|goodid|createtime):([\\w|-]+)";
//        Pattern pattern = Pattern.compile(reg);
//        Matcher matcher = pattern.matcher(str);
//        while (matcher.find()) {
//            System.out.println(matcher.group(0));
//            System.out.println(matcher.group(1));
//            System.out.println(matcher.group(2));
//            System.out.println("***********");
//        }

//        System.out.println(9/10);
    }


}
