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

    // 排序后发送索引信息的队列
    public final LinkedBlockingQueue<Object> orderIndexQueue = new LinkedBlockingQueue<>();
    public final LinkedBlockingQueue<Object> buyerIndexQueue = new LinkedBlockingQueue<>();
    public final LinkedBlockingQueue<Object> goodsIndexQueue = new LinkedBlockingQueue<>();

    // 写hash后的数据的writers
    public final BufferedWriter[][] orderWriters = new BufferedWriter[3][RaceConfig.ORDER_FILE_SIZE];
    public final BufferedWriter[][] buyerWriters = new BufferedWriter[3][RaceConfig.BUYER_FILE_SIZE];
    public final BufferedWriter[][] goodsWriters = new BufferedWriter[3][RaceConfig.GOODS_FILE_SIZE];
    // 一个文件对应一个索引
    public final BufferedWriter[][] orderIndexWriters = new BufferedWriter[3][RaceConfig.ORDER_FILE_SIZE];
    public final BufferedWriter[][] buyerIndexWriters = new BufferedWriter[3][RaceConfig.BUYER_FILE_SIZE];
    public final BufferedWriter[][] goodsIndexWriters = new BufferedWriter[3][RaceConfig.GOODS_FILE_SIZE];

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

    public void init(final Collection<String> storeFolders, final IndexProcessor indexProcessor) throws InterruptedException, IOException {
        // 相同磁盘的路径前缀相同
        threads =  Executors.newFixedThreadPool(5);
        this.indexProcessor = indexProcessor;
        indexProcessor.init();

        execute(orderQueues[0], orderLatch, RaceConfig.DISK1+"o/", RaceConfig.ORDER_FILE_SIZE);
        execute(orderQueues[1], orderLatch, RaceConfig.DISK2+"o/", RaceConfig.ORDER_FILE_SIZE);
        execute(orderQueues[2], orderLatch, RaceConfig.DISK3+"o/", RaceConfig.ORDER_FILE_SIZE);

        // todo 有没有必要让good和order分不到不同磁盘上
        execute(buyerQueue, buyerLatch, RaceConfig.DISK2+"b/",RaceConfig.BUYER_FILE_SIZE);
        execute(goodsQueue, goodsLatch, RaceConfig.DISK3+"g/",RaceConfig.GOODS_FILE_SIZE);

        new Thread(new Runnable() {
            @Override
            public void run() {
                try { //todo 这个地方是构建索引
//                    buyerLatch.await();
//                    for (BufferedWriter bw : buyerWriters) {
//                        if (bw!=null) {
//                            bw.flush();
//                            bw.close();
//                        }
//                    }
//                    sortData(buyerIndexQueue, buyerSortLatch, RaceConfig.BUYER_FILE_SIZE,RaceConfig.STORE_PATH
//                            +"b",false);
//                    indexProcessor.createBuyerIndex(buyerIndexQueue);
//
//                    goodsLatch.await();
//                    for (BufferedWriter bw : goodsWriters) {
//                        if (bw!=null) {
//                            bw.flush();
//                            bw.close();
//                        }
//                    }
//                    sortData(goodsIndexQueue, goodsSortLatch,RaceConfig.GOODS_FILE_SIZE,RaceConfig.STORE_PATH
//                            +"g",false);
//                    indexProcessor.createGoodsIndex(goodsIndexQueue);
//
//                    orderLatch.await();
//                    for (BufferedWriter bw : orderWriters) {
//                        if (bw!=null) {
//                            bw.flush();
//                            bw.close();
//                        }
//                    }
//                    sortData(orderIndexQueue, orderSortLatch, RaceConfig.ORDER_FILE_SIZE,RaceConfig.STORE_PATH
//                            +"o",true);
//                    indexProcessor.addBuyeridAndCreateTime("","","");
//                    indexProcessor.addGoodidToOrderid("","");
//                    indexProcessor.createOrderIndex(orderIndexQueue);
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
        StringBuffer[] data_sbs = new StringBuffer[fileSize];
        StringBuffer[] index_sbs = new StringBuffer[fileSize];
        AtomicInteger[] posCounters = new AtomicInteger[fileSize];

        threads.execute(new ProcessData(queue, dataWriters, indexWriters, data_sbs, index_sbs,
                posCounters, latch, storeFold));
    }

    public void waitOver() throws InterruptedException {
        buyerSortLatch.await();
        buyerIndexQueue.offer(new RecordIndex("","",0,-1));

        goodsSortLatch.await();
        goodsIndexQueue.offer(new RecordIndex("","",0,-1));

        orderSortLatch.await();
        orderIndexQueue.offer(new ArrayList());
        threads.shutdown();
        indexProcessor.waitOver();
    }

    class ProcessData implements Runnable {

        private LinkedBlockingQueue<String[][]> queue;
        private BufferedWriter[] dataWriters;
        private BufferedWriter[] indexWriters;
        private StringBuffer[] data_sbs;
        private StringBuffer[] index_sbs;
        private AtomicInteger[] posCounters;
        private CountDownLatch latch;
        private String storeFold;


        private int fileSize;

        public ProcessData (LinkedBlockingQueue<String[][]> queue, BufferedWriter[] dataWriters,
                            BufferedWriter[] indexWriters, StringBuffer[] data_sbs, StringBuffer[] index_sbs,
                            AtomicInteger[] posCounters, CountDownLatch latch, String storeFold) {
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
                    if (row.length == 2) {
                        id   = row[1][1];
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
                                    break;
                                case "createtime":
                                    createtime = row[i][1];
                                    break;
                            }
                        }
//                                indexProcessor.addBuyeridAndCreateTime(id, createtime, buyerid);
//                                indexProcessor.addGoodidToOrderid(id, goodid);
                    }


                    int index = Math.abs(id.hashCode())%fileSize;
                    String indexStr = ""; // todo 构建index
                    index_sbs[index].append(indexStr).append("\n");
                    data_sbs[index].append(id).append("&").append(row[0][0]).append("\n");
                    if (posCounters[index].get()==200){
                        // 因为要记位置，所以索引的记录是不安全的 因为不同的线程可能会比另外一个线程先到200
                        // pos的值必须是线程安全的 -> 写的东西再加个队列? 另外一个线程专门去写, 起三个线程 3*FileNum
                        // 有个问题值得注意 -> 记录完pos后，在向队列添加的时候可能反而会加到前面或后面去 -> 把key和字符串
                        // 拼接一下，在队列那头写磁盘和处理索引
                        dataWriters[index].write(data_sbs[index].toString());
                        data_sbs[index].delete(0,data_sbs[index].length());
                        indexWriters[index].write(index_sbs[index].toString());
                        index_sbs[index].delete(0,index_sbs[index].length());
                        posCounters[index].set(0);
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
