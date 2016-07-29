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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by sxian.wang on 2016/7/19.
 */
public class FileProcessor {

    // 接收原始数据的队列
    public LinkedBlockingQueue<String>[] orderQueues;
    public LinkedBlockingQueue<String> buyerQueue;
    public LinkedBlockingQueue<String> goodsQueue;

    public LinkedBlockingQueue<String[]> hbIndexQueue = new LinkedBlockingQueue<>(50000);
    public LinkedBlockingQueue<String[]> hgIndexQueue = new LinkedBlockingQueue<>(50000);
    public LinkedBlockingQueue<String[]> orderIndexQueue = new LinkedBlockingQueue<>(50000);

    public CountDownLatch orderLatch = new CountDownLatch(3); // todo 关了hg hb
    public CountDownLatch buyerLatch = new CountDownLatch(1);
    public CountDownLatch goodsLatch = new CountDownLatch(1);

    private ExecutorService threads;
    private IndexProcessor indexProcessor;

    public FileProcessor(OrderSystemImpl osi) {
        orderQueues = osi.orderQueues;
        buyerQueue = osi.buyerQueue;
        goodsQueue = osi.goodsQueue;
    }

    public void init(final long start, final IndexProcessor indexProcessor) throws InterruptedException, IOException {
        this.indexProcessor = indexProcessor;
        indexProcessor.init(hbIndexQueue,hgIndexQueue,orderIndexQueue);
        threads = Executors.newFixedThreadPool(6);

        threads.execute(new ProcessOrderData(orderQueues[0], orderLatch,RaceConfig.ORDER_FILE_SIZE,RaceConfig.DISK1+"o/"));
        threads.execute(new ProcessOrderData(orderQueues[1], orderLatch,RaceConfig.ORDER_FILE_SIZE,RaceConfig.DISK2+"o/"));
        threads.execute(new ProcessOrderData(orderQueues[2], orderLatch,RaceConfig.ORDER_FILE_SIZE,RaceConfig.DISK3+"o/"));

        // todo 有没有必要让good和order分不到不同磁盘上
        threads.execute(new ProcessData(buyerQueue, buyerLatch,RaceConfig.BUYER_FILE_SIZE,RaceConfig.DISK1+"b/",true));
        threads.execute(new ProcessData(goodsQueue, goodsLatch,RaceConfig.GOODS_FILE_SIZE,RaceConfig.DISK2+"g/",false));

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    buyerLatch.await();
                    System.out.println("buyer index writer complete, now time: "+(System.currentTimeMillis()-start));
                    indexProcessor.createBuyerIndex();

                    goodsLatch.await();
                    System.out.println("goods index writer complete, now time: "+(System.currentTimeMillis()-start));
                    indexProcessor.createGoodsIndex();

                    orderLatch.await();
                    hbIndexQueue.offer(new String[0]);
                    hgIndexQueue.offer(new String[0]);
                    orderIndexQueue.offer(new String[0],60,TimeUnit.SECONDS);
                    hbIndexQueue = null;
                    hgIndexQueue = null;
                    orderIndexQueue = null;
                    System.out.println("order index writer complete, free memory: "+Runtime.getRuntime().freeMemory()/(1024.0*1024.0)+
                            ", now time: "+(System.currentTimeMillis()-start));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public void waitOver() throws InterruptedException {
        buyerLatch.await();
        goodsLatch.await();
        orderLatch.await();
        buyerLatch = null;
        goodsLatch = null;
        orderLatch = null;
        threads.shutdown();
        indexProcessor.waitOver();
    }

    class ProcessData implements Runnable {
        private boolean flag;
        private int fileSize;
        private String storeFold;
        private CountDownLatch latch;
        private LinkedBlockingQueue<String> queue;

        private BufferedWriter[] dataWriters;
        private BufferedWriter[] indexWriters;
        private StringBuilder[] data_sbs;
        private StringBuilder[] index_sbs;
        private int[] posCounters;
        private int[] counters;



        public ProcessData (LinkedBlockingQueue<String> queue, CountDownLatch latch,int fileSize, String storeFold,
                            boolean flag) throws IOException {
            this.flag = flag;
            this.queue = queue;
            this.latch = latch;
            this.fileSize = fileSize;
            this.storeFold = storeFold;
            init();
        }

        private void init() throws IOException {
            dataWriters = new BufferedWriter[fileSize];
            indexWriters = new BufferedWriter[fileSize];
            data_sbs = new StringBuilder[fileSize];
            index_sbs = new StringBuilder[fileSize];
            posCounters = new int[fileSize];
            counters = new int[fileSize];

            for (int i = 0;i<fileSize;i++) {
                dataWriters[i] = Utils.createWriter(storeFold+i);
                data_sbs[i] = new StringBuilder();
                indexWriters[i] = Utils.createWriter(storeFold+"i"+i);
                index_sbs[i] = new StringBuilder();
                posCounters[i] = 0;
                counters[i] = 0;
            }
        }

        @Override
        public void run() {
            try {
                while (true) {
                    String row = queue.take();
                    if (row.equals("")) {
                        break;
                    }
                    String id = null;
                    if (flag) { // buyerid
                        id = row.substring(8,28);
                    } else {
                        if (row.charAt(27) == '\t') {
                            id = row.substring(7,27);
                        } else {
                            id = row.substring(7,28);
                        }
                    }
                    int index = Math.abs(id.hashCode())%fileSize;
                    int length = row.getBytes().length;
                    int pos = posCounters[index];
                    posCounters[index] += length;
                    data_sbs[index].append(row);
                    String indexStr = id+","+pos+","+length;
                    index_sbs[index].append(indexStr).append("\n");
                    if (counters[index]++ == 200){
                        dataWriters[index].write(data_sbs[index].toString());
                        data_sbs[index].delete(0,data_sbs[index].length());
                        indexWriters[index].write(index_sbs[index].toString());
                        index_sbs[index].delete(0,index_sbs[index].length());
                        counters[index] = 0;
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

    class ProcessOrderData implements Runnable {

        private int fileSize;
        private String storeFold;
        private CountDownLatch latch;
        private LinkedBlockingQueue<String> queue;

        private int[] counter;
        private int[] posCounters;
        private StringBuilder[] data_sbs;
        private BufferedWriter[] dataWriters;


        public ProcessOrderData (LinkedBlockingQueue<String> queue,  CountDownLatch latch,
                                 int fileSize, String storeFold) throws IOException {
            this.queue = queue;
            this.latch = latch;
            this.fileSize = fileSize;
            this.storeFold = storeFold;
            init();
        }

        private void init() throws IOException {
            counter = new int[fileSize];
            posCounters = new int[fileSize];
            dataWriters = new BufferedWriter[fileSize];
            data_sbs = new StringBuilder[fileSize];
            for (int i = 0;i<fileSize;i++) {
                counter[i] = 0;
                posCounters[i] = 0;
                dataWriters[i] = Utils.createWriter(storeFold+i);
                data_sbs[i] = new StringBuilder();
            }
        }

        @Override
        public void run() {
            try {
                int c = 0;
                while (true) {
                    String row = queue.take();
                    if (row.equals("")) {
                        break;
                    }
                    c++;

                    String orderid,goodid;
                    orderid = row.substring(8,17); // orderid
                    try {
                        int in = row.indexOf("goodid")+7;
                        goodid = row.substring(in,row.indexOf("\t",in));
                    } catch (RuntimeException e1) {
                        System.out.println("error: " + row);
                        continue;
                    }

                    // buyerid -> orderid
                    hbIndexQueue.offer(new String[]{row.substring(48,68),orderid+","+row.substring(29,39)},60, TimeUnit.SECONDS);
                    // goodid -> orderid
                    hgIndexQueue.offer(new String[]{goodid,orderid},60, TimeUnit.SECONDS);

                    // 订单信息, 单线程处理
                    int index = Math.abs(Math.abs(goodid.hashCode()))%fileSize; // -> order按照goodid hash，buyer 和goods按照主键
                    int length = row.getBytes().length;
                    int pos = posCounters[index];
                    orderIndexQueue.offer(new String[]{orderid,storeFold+index+","+pos+","+length},60, TimeUnit.SECONDS);
                    posCounters[index] += length;
                    data_sbs[index].append(row);
                    if (counter[index]++ == 200){
                        // 因为要记位置，所以索引的记录是不安全的 因为不同的线程可能会比另外一个线程先到200
                        // pos的值必须是线程安全的 -> 写的东西再加个队列? 另外一个线程专门去写, 起三个线程 3*FileNum
                        // 有个问题值得注意 -> 记录完pos后，在向队列添加的时候可能反而会加到前面或后面去 -> 把key和字符串
                        // 拼接一下，在队列那头写磁盘和处理索引
                        dataWriters[index].write(data_sbs[index].toString());
                        data_sbs[index].delete(0,data_sbs[index].length());
                        counter[index] = 0;
                    }
                }
                for (int i = 0;i<fileSize;i++) {
                    dataWriters[i].write(data_sbs[i].toString().toCharArray());
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
                try {
                    for (int i = 0;i<fileSize;i++) {
                        if (dataWriters[i] != null) {
                            dataWriters[i].flush();
                            dataWriters[i].close();
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
