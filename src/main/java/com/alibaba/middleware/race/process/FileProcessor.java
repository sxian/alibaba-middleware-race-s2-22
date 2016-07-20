package com.alibaba.middleware.race.process;

import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.RaceConfig;

import java.io.*;
import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

/**
 * Created by sxian.wang on 2016/7/19.
 */
public class FileProcessor {
    public final LinkedBlockingQueue<OrderSystemImpl.Row> orderQueue = OrderSystemImpl.orderQueue;
    public final LinkedBlockingQueue<OrderSystemImpl.Row> buyerQueue = OrderSystemImpl.buyerQueue;
    public final LinkedBlockingQueue<OrderSystemImpl.Row> goodsQueue = OrderSystemImpl.goodsQueue;

    public void init(final Collection<String> storeFiles) throws InterruptedException {
        new Thread(new Runnable() {
            @Override
            public void run() {
                BufferedWriter br = null;
                try {
                    br = createWriter(RaceConfig.STORE_PATH+"order.txt");
                    while (true) {
                        br.write(orderQueue.take().toString());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        br.flush();
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                BufferedWriter br = null;
                try {
                    br = createWriter(RaceConfig.STORE_PATH+"buyer.txt");
                    while (true) {
                        br.write(buyerQueue.take().toString());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        br.flush();
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                BufferedWriter br = null;
                try {
                    br = createWriter(RaceConfig.STORE_PATH+"goods.txt");
                    while (true) {
//                        String str = goodsQueue.take().toString();
                        br.write(goodsQueue.take().toString());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    public void ProcessCase(String filePath) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(new File(RaceConfig.DATA_ROOT+"order.2.2")));
        String record = br.readLine();
        while (record!=null) {
            if ((record.equals("}")||record.equals(""))) { // todo 执行查询
                record = br.readLine();
                continue;
            }

            String[] kv = record.split(":");
            switch (kv[0]) {
                case "CASE":

                    break;
                case "ORDERID":
                    break;
                case "SALERID":
                    break;
                case "GOODID":
                    break;
                case "KEYS":
                    break;
                case "STARTTIME":
                    break;
                case "ENDTIME":
                    break;
                case "Result":
                    break;
            }
            record = br.readLine();
        }
        br.close();
    }


    private BufferedWriter createWriter(String file) throws IOException {
        return new BufferedWriter(new FileWriter(file));
    }
}
