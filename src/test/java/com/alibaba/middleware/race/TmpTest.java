package com.alibaba.middleware.race;

import com.alibaba.middleware.race.datastruct.BplusTree;
import com.alibaba.middleware.race.util.Utils;
import org.omg.SendingContext.RunTime;

import java.io.BufferedReader;
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

        new Thread(new Runnable() {
            @Override
            public void run() {
                BplusTree bpt = new BplusTree(150);
                int count = 0;
                while (true) {
                    try {
                        String line = queue.take();
                        if (line.equals("")) {
                            break;
                        }
                        int flag0 = line.indexOf(",",11)+1;
                        int flag = line.indexOf(",",flag0);// path 的逗号的位置
                        count++;
                        bpt.insertOrUpdate(line.substring(0,10),new Object[] {line.substring(11,line.indexOf(",",11)),
                                line.substring(flag0,flag),line.substring(flag+1,line.length())});
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                latch.countDown();;
                System.out.println("+++ "+Runtime.getRuntime().freeMemory()/M+" +++");
                queue = null;
                System.gc();
                System.out.println("+++ "+Runtime.getRuntime().freeMemory()/M+" +++");
                bpt = null;
                System.gc();
                System.out.println("+++ "+Runtime.getRuntime().freeMemory()/M+" +++");
            }
        }).start();

        String path = "t/d1/o/i";
        long start = System.currentTimeMillis();
        System.out.println(Runtime.getRuntime().freeMemory()/M);
        int count = 0;
        for (int i = 0;i<41;i++) {
            BufferedReader br = Utils.createReader(path+i);
            String line = br.readLine();
            while (line!=null) {
                for (int j = 0;j<31;j++) {
                    count++;
                    queue.offer(j+line,60, TimeUnit.SECONDS);
                }
                line = br.readLine();
            }
            br.close();
        }
        queue.offer("");
        latch.await();
        System.out.println(System.currentTimeMillis()-start);
//        System.out.println(Runtime.getRuntime().freeMemory()/M);
//        System.out.println("************************************");
//        System.gc();
//        Thread.sleep(1000);
//        System.out.println(Runtime.getRuntime().freeMemory()/M);
//        System.out.println("************************************");
//        bpt = null;
//        System.gc();
//        System.out.println(Runtime.getRuntime().freeMemory()/M);

//        long leg = raf.length();
//        raf.seek(1986841); // ap-804e-6f1c7d177abd,25468,470
//        byte[] bytes = new byte[195];
//        raf.read(bytes);
//        System.out.println(new String(bytes));
//        Utils.spilitCaseFile(DATA_ROOT+"case.0",RaceConfig.STORE_PATH,10);

        //                    int flag0 = line.indexOf(",",10)+1;
//                    int flag = line.indexOf(",",flag0);// path 的逗号的位置
//                    count++;
//                    bpt.insertOrUpdate(line.substring(0,9)+j,new Object[] {line.substring(10,line.indexOf(",",10)),
//                            Integer.valueOf(line.substring(flag0,flag)),Integer.valueOf(line.substring(flag+1,line.length()))});

    }



}
