package com.alibaba.middleware.race;

import com.alibaba.middleware.race.util.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

/**
 * Created by sxian.wang on 2016/7/23.
 */
public class TmpTest {
    public static float M = 1024 * 1024;
    public static void main(String[] args) throws IOException, InterruptedException {
        BufferedReader br = Utils.createReader("t/d1/b/iS0");
        String line = br.readLine();
        int[][] objects = new int[8000000][2];
        int count = 0;
        System.out.println(Runtime.getRuntime().freeMemory()/M);
        while (line!=null) {
            String[] indexs = line.split(" ");
            for (String index : indexs) {
                for (int i = 0;i<10000;i++) {
                    int first = index.indexOf(",");
                    int last  = index.lastIndexOf(",");
                    String key = index.substring(0,first);
                    int pos = Integer.valueOf(index.substring(first+1,last));
                    int len = Integer.valueOf(index.substring(last+1));
                    objects[count++] = new int[]{pos,len};
                }
            }
            line = br.readLine();
        }
        System.gc();
        System.out.println(Runtime.getRuntime().freeMemory()/M);
        int a = 1;
    }
}
