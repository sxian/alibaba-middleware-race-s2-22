package com.alibaba.middleware.race;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by sxian.wang on 2016/7/23.
 */
public class TmpTest {
    public static float M = 1024 * 1024;
    public static void main(String[] args) throws IOException, InterruptedException {
        String str = "14321014175";
        System.out.println(Math.abs(str.hashCode()%3));
        System.out.println(Math.abs(str.hashCode()%RaceConfig.ORDER_FILE_SIZE));

        String str1 = "10109881096";
        System.out.println(Math.abs(str1.hashCode()%3));
        System.out.println(Math.abs(str1.hashCode()%RaceConfig.ORDER_FILE_SIZE));

        byte[] bytes = new byte[100];
        RandomAccessFile raf = new RandomAccessFile("1","r");
        FileChannel channel = raf.getChannel();
        MappedByteBuffer mbb = channel.map(FileChannel.MapMode.READ_ONLY,0,channel.size());
        mbb.get(100);
        str.lastIndexOf("a");
        raf.read(bytes);
        raf.seek(1);
    }
}
