package com.alibaba.middleware.race;

import java.io.IOException;

/**
 * Created by sxian.wang on 2016/7/23.
 */
public class TmpTest {
    public static void main(String[] args) throws IOException {
        double a = 1.00;
        String b = "1.520";
        long c = Long.valueOf(b);
        System.out.println(b.contains("."));
        System.out.println(c);
    }
}
