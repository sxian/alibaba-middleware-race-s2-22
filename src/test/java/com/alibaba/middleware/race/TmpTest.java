package com.alibaba.middleware.race;

import com.alibaba.middleware.race.datastruct.BplusTree;
import com.alibaba.middleware.race.util.Utils;
import org.omg.SendingContext.RunTime;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.alibaba.middleware.race.RaceConfig.DATA_ROOT;

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

    }
}
