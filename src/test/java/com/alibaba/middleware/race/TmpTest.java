package com.alibaba.middleware.race;

import com.alibaba.middleware.race.util.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by sxian.wang on 2016/7/23.
 */
public class TmpTest {
    public static void main(String[] args) throws IOException {
//        RandomAccessFile raf = new RandomAccessFile("t/oS0","r");
//        raf.seek(32597922);
//        byte[] bytes = new byte[187];
//        raf.read(bytes);
//        System.out.println(new String(bytes));
        String[]  orderFiles = new String[]{RaceConfig.DATA_ROOT+"order.0.0", RaceConfig.DATA_ROOT+"order.0.3",
                RaceConfig.DATA_ROOT+"order.1.1", RaceConfig.DATA_ROOT+"order.2.2"};
        HashMap<String,String> map = new HashMap<>();
        String[] strs = new String[400000];
        ArrayList<String> rows = new ArrayList<>();
        long start =System.currentTimeMillis();
        for (int a = 0;a<1;a++) {
            int i = 0;
            for (String file : orderFiles) {
                BufferedReader br = Utils.createReader(file);
                String str = br.readLine();
                while (str!=null) {
//                    OrderSystemImpl.Row row = OrderSystemImpl.createRow(str);
    //                map.put("sdfsdfasdsd",str);
                    map.put(String.valueOf(i++),str);
//                    strs[400000*a+i++] = str;
                    str = br.readLine();
                }
                System.gc();
                br.close();
            }
        }
        System.out.println("build test data complete: "+(System.currentTimeMillis()-start)+", start query...");

    }
}
