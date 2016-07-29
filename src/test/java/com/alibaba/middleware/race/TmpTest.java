package com.alibaba.middleware.race;

import com.alibaba.middleware.race.datastruct.BplusTree;
import com.alibaba.middleware.race.util.Utils;
import org.omg.SendingContext.RunTime;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.alibaba.middleware.race.RaceConfig.DATA_ROOT;

/**
 * Created by sxian.wang on 2016/7/23.
 */
public class TmpTest {
    public static float M = 1024 * 1024;
    public static LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>(10000);
    public static CountDownLatch latch = new CountDownLatch(1);
    public static void main(String[] args) throws IOException, InterruptedException {
        BufferedReader br = Utils.createReader("prerun_data/test.txt");
        String orderid,
                buyerid,
                goodid,
                createtime;

        String row = br.readLine();
        while (row!=null) {
            int orderid_of = row.indexOf("orderid")+8;
            int buyerid_of = row.indexOf("buyerid")+8;
            int goodid_of = row.indexOf("goodid")+7;
            int createtime_of = row.indexOf("createtime")+11;

            orderid = row.indexOf("\t",orderid_of) != -1 ? row.substring(orderid_of,row.indexOf("\t",orderid_of)) :
                    row.substring(orderid_of,row.length());
            buyerid = row.indexOf("\t",buyerid_of) != -1 ? row.substring(buyerid_of,row.indexOf("\t",buyerid_of)) :
                    row.substring(buyerid_of,row.length());
            goodid = row.indexOf("\t",goodid_of) != -1 ? row.substring(goodid_of,row.indexOf("\t",goodid_of)) :
                    row.substring(goodid_of,row.length());
            createtime = row.indexOf("\t",createtime_of) != -1 ? row.substring(createtime_of,row.indexOf("\t",createtime_of)) :
                    row.substring(createtime_of,row.length());
            row = br.readLine();
        }

    }



}
