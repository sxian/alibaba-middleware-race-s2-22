package com.alibaba.middleware.race;

import java.io.*;
import java.util.Collection;
import java.util.Iterator;

/**
 * Created by sxian.wang on 2016/7/19.
 */
public class OrderSystemImplTest {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(
                                new FileReader(
                                        new File("prerun_data/case.0")));
        String str = br.readLine();


        long startTime = System.currentTimeMillis();
        int count = 0;
        while (str != null) {
            count++;
//            System.out.println(str);
            str = br.readLine();
        }
        System.out.println("Use time: " + (System.currentTimeMillis() - startTime));
        System.out.println("Record Count: " + count);
    }
}
