package com.alibaba.middleware.race.process;

import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.datastruct.BplusTree;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.*;

/**
 * Created by sxian.wang on 2016/7/19.
 */
public class FileProcessor {

    public final IndexProcessor indexProcessor = new IndexProcessor();

    public final ArrayList<LinkedBlockingQueue<OrderSystemImpl.Row>> orderQueues = OrderSystemImpl.orderQueues;
    public final ArrayList<LinkedBlockingQueue<OrderSystemImpl.Row>> buyerQueues = OrderSystemImpl.buyerQueues;
    public final ArrayList<LinkedBlockingQueue<OrderSystemImpl.Row>> goodsQueues = OrderSystemImpl.goodsQueues;

    public final BufferedWriter[] orderWriters = new BufferedWriter[RaceConfig.ORDER_FILE_SIZE];
    public final BufferedWriter[] buyerWriters = new BufferedWriter[RaceConfig.BUYER_FILE_SIZE];
    public final BufferedWriter[] goodsWriters = new BufferedWriter[RaceConfig.GOODS_FILE_SIZE];

    public final String[] storeDiskPath = new String[3];

    public final CountDownLatch latch = new CountDownLatch(orderQueues.size()+buyerQueues.size()+goodsQueues.size());
    private ExecutorService threads;

    public void init(final Collection<String> storeFolders) throws InterruptedException {// todo 确认下folders的数量
        // 相同磁盘的路径前缀相同
        threads =  Executors.newFixedThreadPool(orderQueues.size()+buyerQueues.size()+goodsQueues.size());
        if (RaceConfig.ONLINE) {
            for (String path : storeFolders) {
            // todo 创建文件夹
            }
        } else {
            execute(orderQueues,orderWriters,RaceConfig.ORDER_FILE_SIZE,"goodid",RaceConfig.STORE_PATH+"orderdata");
            execute(buyerQueues,buyerWriters,RaceConfig.BUYER_FILE_SIZE,"buyerid",RaceConfig.STORE_PATH+"buyerdata");
            execute(goodsQueues,goodsWriters,RaceConfig.GOODS_FILE_SIZE,"goodid",RaceConfig.STORE_PATH+"goodsdata");
        }
    }

    public void execute(ArrayList<LinkedBlockingQueue<OrderSystemImpl.Row>> queues, final BufferedWriter[] writers,
                        final int fileSize, final String key,final String pathPrefix) {
        for (int i = 0;i<queues.size();i++) {
            final LinkedBlockingQueue<OrderSystemImpl.Row> queue = queues.get(i);
            threads.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        while (true) {
                            OrderSystemImpl.Row row = queue.take();
                            if (row.size() == 0) {
                                break;
                            }
                            int index = Math.abs(row.get(key).valueAsString().hashCode())%fileSize;
                            if(writers[index] != null) {
                                writers[index].write(row.toString());
                            } else {
                                writers[index] = createWriter(pathPrefix+index);
                                writers[index].write(row.toString());
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
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

    public void waitOver() throws InterruptedException {
        latch.await();
        try {
            for (BufferedWriter bw : orderWriters) {
                if (bw!=null) {
                    bw.flush();
                    bw.close();
                }
            }
            for (BufferedWriter bw : buyerWriters) {
                if (bw!=null) {
                    bw.flush();
                    bw.close();
                }
            }
            for (BufferedWriter bw : goodsWriters) {
                if (bw!=null) {
                    bw.flush();
                    bw.close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        threads.shutdown();
    }
}
