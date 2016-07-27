package com.alibaba.middleware.race.process;

import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.datastruct.BplusTree;
import com.alibaba.middleware.race.datastruct.Node;
import com.alibaba.middleware.race.datastruct.RecordIndex;
import com.alibaba.middleware.race.util.Utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by sxian.wang on 2016/7/19.
 */
public class FileProcessor {

    // 接收原始数据的队列
    public final LinkedBlockingQueue<String[][]>[] orderQueues = OrderSystemImpl.orderQueues;
    public final LinkedBlockingQueue<String[][]> buyerQueue = OrderSystemImpl.buyerQueue;
    public final LinkedBlockingQueue<String[][]> goodsQueue = OrderSystemImpl.goodsQueue;

    // hash完相对应的所有文件后开始构建索引(排序)
    public final CountDownLatch orderLatch = new CountDownLatch(3);
    public final CountDownLatch buyerLatch = new CountDownLatch(1);
    public final CountDownLatch goodsLatch = new CountDownLatch(1);

    // 所有的索引文件排序完成后退出fileProcessor
    public final CountDownLatch orderSortLatch = new CountDownLatch(RaceConfig.ORDER_FILE_SIZE);
    public final CountDownLatch buyerSortLatch = new CountDownLatch(RaceConfig.BUYER_FILE_SIZE);
    public final CountDownLatch goodsSortLatch = new CountDownLatch(RaceConfig.GOODS_FILE_SIZE);

    private ExecutorService threads;
    private IndexProcessor indexProcessor;

    public void init(final long start, final IndexProcessor indexProcessor) throws InterruptedException, IOException {
        // 相同磁盘的路径前缀相同
        threads =  Executors.newFixedThreadPool(5);
        this.indexProcessor = indexProcessor;
//        indexProcessor.init();

        execute(orderQueues[0], orderLatch, RaceConfig.DISK1+"o/", RaceConfig.ORDER_FILE_SIZE);
        execute(orderQueues[1], orderLatch, RaceConfig.DISK2+"o/", RaceConfig.ORDER_FILE_SIZE);
        execute(orderQueues[2], orderLatch, RaceConfig.DISK3+"o/", RaceConfig.ORDER_FILE_SIZE);

        // todo 有没有必要让good和order分不到不同磁盘上
        execute(buyerQueue, buyerLatch, RaceConfig.DISK1+"b/",RaceConfig.BUYER_FILE_SIZE);
        execute(goodsQueue, goodsLatch, RaceConfig.DISK2+"g/",RaceConfig.GOODS_FILE_SIZE);

        new Thread(new Runnable() {
            @Override
            public void run() {
                try { //todo 这个地方是构建索引 -> 在indexProcess里做, 如果这样的话线程池就没用了
                    buyerLatch.await();
                    System.out.println("buyer index writer complete, now time: "+(System.currentTimeMillis()-start));

//                    indexProcessor.createBuyerIndex();
//
                    goodsLatch.await();
                    System.out.println("goods index writer complete, now time: "+(System.currentTimeMillis()-start));

//                    indexProcessor.createGoodsIndex();
//
                    orderLatch.await();
                    System.out.println("order index writer complete, now time: "+(System.currentTimeMillis()-start));

//                    indexProcessor.addBuyeridAndCreateTime("","",""); -> 再想想咋搞
//                    indexProcessor.addGoodidToOrderid("","");
//                    indexProcessor.createOrderIndex();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }


    public void execute(LinkedBlockingQueue<String[][]> queue, CountDownLatch latch, String storeFold,
                        int fileSize) throws IOException {

        BufferedWriter[] dataWriters = new BufferedWriter[fileSize];
        BufferedWriter[] indexWriters = new BufferedWriter[fileSize];
//        StringBuffer[] data_sbs = new StringBuffer[fileSize]; // todo有没有必要开多线程
//        StringBuffer[] index_sbs = new StringBuffer[fileSize];
        StringBuilder[] data_sbs = new StringBuilder[fileSize];
        StringBuilder[] index_sbs = new StringBuilder[fileSize];
//        AtomicInteger[] posCounters = new AtomicInteger[fileSize];
        int[] posCounters = new int[fileSize];

        for (int i = 0;i<fileSize;i++) {
            dataWriters[i] = Utils.createWriter(storeFold+i);
            indexWriters[i] = Utils.createWriter(storeFold+"i"+i);
            data_sbs[i] = new StringBuilder();
            index_sbs[i] = new StringBuilder();
            posCounters[i] = 0/*new AtomicInteger(0)*/;
        }
        threads.execute(new ProcessData(queue, dataWriters, indexWriters, data_sbs, index_sbs,
                posCounters, latch, storeFold));
    }

    public void waitOver() throws InterruptedException {
        buyerLatch.await();

        goodsLatch.await();

        orderLatch.await();
        threads.shutdown();
//        indexProcessor.waitOver();
    }

    class ProcessData implements Runnable {

        private LinkedBlockingQueue<String[][]> queue;
        private BufferedWriter[] dataWriters;
        private BufferedWriter[] indexWriters;
        private StringBuilder[] data_sbs;
        private StringBuilder[] index_sbs;
//        private AtomicInteger[] posCounters;
        private int[] posCounters;

        private CountDownLatch latch;
        private String storeFold;


        private int fileSize;

        public ProcessData (LinkedBlockingQueue<String[][]> queue, BufferedWriter[] dataWriters,
                            BufferedWriter[] indexWriters, StringBuilder[] data_sbs, StringBuilder[] index_sbs,
                            /*AtomicInteger[]*/int[] posCounters, CountDownLatch latch, String storeFold) {
            this.queue = queue;
            this.dataWriters = dataWriters;
            this.indexWriters = indexWriters;
            this.data_sbs = data_sbs;
            this.index_sbs = index_sbs;
            this.posCounters = posCounters;
            fileSize = dataWriters.length;
            this.latch = latch;
            this.storeFold = storeFold;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    String[][] row = queue.take();
                    if (row.length==0) {
                        break;
                    }
                    String id = "";
                    int hashcode = 0;
                    if (row.length == 2) {
                        id   = row[1][1];
                        hashcode = row[1][1].hashCode();
                    } else {
                        String buyerid,goodid,createtime;
                        buyerid=goodid=createtime = "";
                        for (int i = 1; i<5; i++) {
                            switch (row[i][0]) {
                                case "orderid":
                                    id = row[i][1];
                                    break;
                                case "buyerid":
                                    buyerid = row[i][1];
                                    break;
                                case "goodid":
                                    goodid = row[i][1];
                                    hashcode = row[i][1].hashCode();
                                    break;
                                case "createtime":
                                    createtime = row[i][1];
                                    break;
                            }
                        }
//                                indexProcessor.addBuyeridAndCreateTime(id, createtime, buyerid);
//                                indexProcessor.addGoodidToOrderid(id, goodid);
                    }

                    int index = Math.abs(hashcode)%fileSize; // -> order按照goodid hash，buyer 和goods按照主键
                    int length = row[0][0].getBytes().length;
                    int pos = posCounters[index]++/*getAndAdd(length)*/;
                    data_sbs[index].append(row[0][0]);
                    String indexStr = id+","+storeFold+index+","+pos+","+length; // todo 构建index
                    index_sbs[index].append(indexStr).append("\n");
                    if (posCounters[index]/*.get()*/==200){
                        // 因为要记位置，所以索引的记录是不安全的 因为不同的线程可能会比另外一个线程先到200
                        // pos的值必须是线程安全的 -> 写的东西再加个队列? 另外一个线程专门去写, 起三个线程 3*FileNum
                        // 有个问题值得注意 -> 记录完pos后，在向队列添加的时候可能反而会加到前面或后面去 -> 把key和字符串
                        // 拼接一下，在队列那头写磁盘和处理索引
                        dataWriters[index].write(data_sbs[index].toString());
                        data_sbs[index].delete(0,data_sbs[index].length());
                        indexWriters[index].write(index_sbs[index].toString());
                        index_sbs[index].delete(0,index_sbs[index].length());
                        posCounters[index] = 0;/*.set(0)*/;
                    }
                }
                for (int i = 0;i<fileSize;i++) {
                    dataWriters[i].write(data_sbs[i].toString().toCharArray());
                    indexWriters[i].write(index_sbs[i].toString());
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
                try {
                    latch.await();
                    for (int i = 0;i<fileSize;i++) {
                        if (dataWriters[i] != null) {
                            dataWriters[i].flush();
                            dataWriters[i].close();
                        }

                        if (indexWriters[i] != null) {
                            indexWriters[i].flush();
                            indexWriters[i].close();
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
