package com.alibaba.middleware.race;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by sxian.wang on 2016/7/23.
 */
public class TmpTest {
    public static void main(String[] args) throws IOException {
        RandomAccessFile raf = new RandomAccessFile("t/oS0","r");
        raf.seek(32597922);
        byte[] bytes = new byte[187];
        raf.read(bytes);
        System.out.println(new String(bytes));
    }
}
