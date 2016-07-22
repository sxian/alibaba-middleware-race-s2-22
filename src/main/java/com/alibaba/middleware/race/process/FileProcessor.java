package com.alibaba.middleware.race.process;

import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.datastruct.BplusTree;
import com.alibaba.middleware.race.datastruct.Node;
import com.alibaba.middleware.race.datastruct.RecordIndex;
import com.alibaba.middleware.race.util.Utils;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static javafx.scene.input.KeyCode.R;

/**
 * Created by sxian.wang on 2016/7/19.
 */
public class FileProcessor {

    // 接收原始数据的队列
    public final ArrayList<LinkedBlockingQueue<OrderSystemImpl.Row>> orderQueues = OrderSystemImpl.orderQueues;
    public final ArrayList<LinkedBlockingQueue<OrderSystemImpl.Row>> buyerQueues = OrderSystemImpl.buyerQueues;
    public final ArrayList<LinkedBlockingQueue<OrderSystemImpl.Row>> goodsQueues = OrderSystemImpl.goodsQueues;

    // 排序后发送索引信息的队列
    public final LinkedBlockingQueue<Object> orderIndexQueue = new LinkedBlockingQueue<>();
    public final LinkedBlockingQueue<Object> buyerIndexQueue = new LinkedBlockingQueue<>();
    public final LinkedBlockingQueue<Object> goodsIndexQueue = new LinkedBlockingQueue<>();

    // 写hash后的数据的writers
    public final BufferedWriter[] orderWriters = new BufferedWriter[RaceConfig.ORDER_FILE_SIZE];
    public final BufferedWriter[] buyerWriters = new BufferedWriter[RaceConfig.BUYER_FILE_SIZE];
    public final BufferedWriter[] goodsWriters = new BufferedWriter[RaceConfig.GOODS_FILE_SIZE];


    // hash完相对应的所有文件后开始排序
    public final CountDownLatch orderLatch = new CountDownLatch(orderQueues.size());
    public final CountDownLatch buyerLatch = new CountDownLatch(buyerQueues.size());
    public final CountDownLatch goodsLatch = new CountDownLatch(goodsQueues.size());
    // 所有的文件排序完成后退出fileProcessor
    public final CountDownLatch orderSortLatch = new CountDownLatch(RaceConfig.ORDER_FILE_SIZE);
    public final CountDownLatch buyerSortLatch = new CountDownLatch(RaceConfig.BUYER_FILE_SIZE);
    public final CountDownLatch goodsSortLatch = new CountDownLatch(RaceConfig.GOODS_FILE_SIZE);

    // 不同硬盘上的存储路径
    public final String[] storeDiskPath = new String[3];

    private ExecutorService threads;
    private IndexProcessor indexProcessor;

    public void init(final Collection<String> storeFolders, final IndexProcessor indexProcessor) throws InterruptedException, IOException {// todo 确认下folders的数量
        // 相同磁盘的路径前缀相同
        threads =  Executors.newFixedThreadPool(orderQueues.size()+buyerQueues.size()+goodsQueues.size());
        this.indexProcessor = indexProcessor;

        if (RaceConfig.ONLINE) {
            for (String path : storeFolders) {
            // todo 创建文件夹
            }
        } else {
            execute(orderQueues,orderWriters, orderLatch, RaceConfig.ORDER_FILE_SIZE,"orderid",
                    RaceConfig.STORE_PATH+"o",true);
            execute(buyerQueues,buyerWriters, buyerLatch, RaceConfig.BUYER_FILE_SIZE,"buyerid",
                    RaceConfig.STORE_PATH+"b",false);
            execute(goodsQueues,goodsWriters, goodsLatch, RaceConfig.GOODS_FILE_SIZE,"goodid",
                    RaceConfig.STORE_PATH+"g",false);
        }

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    buyerLatch.await();
                    for (BufferedWriter bw : buyerWriters) {
                        if (bw!=null) {
                            bw.flush();
                            bw.close();
                        }
                    }
                    sortData(buyerIndexQueue, buyerSortLatch, RaceConfig.BUYER_FILE_SIZE,RaceConfig.STORE_PATH
                            +"b",false);
                    indexProcessor.createBuyerIndex(buyerIndexQueue);

                    goodsLatch.await();
                    for (BufferedWriter bw : goodsWriters) {
                        if (bw!=null) {
                            bw.flush();
                            bw.close();
                        }
                    }
                    sortData(goodsIndexQueue, goodsSortLatch,RaceConfig.GOODS_FILE_SIZE,RaceConfig.STORE_PATH
                            +"g",false);
                    indexProcessor.createGoodsIndex(goodsIndexQueue);

                    orderLatch.await();
                    for (BufferedWriter bw : orderWriters) {
                        if (bw!=null) {
                            bw.flush();
                            bw.close();
                        }
                    }
                    sortData(orderIndexQueue, orderSortLatch, RaceConfig.ORDER_FILE_SIZE,RaceConfig.STORE_PATH
                            +"o",true);
                    indexProcessor.addBuyeridAndCreateTime("","","");
                    indexProcessor.addGoodidToOrderid("","");
                    indexProcessor.createOrderIndex(orderIndexQueue);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public void execute(ArrayList<LinkedBlockingQueue<OrderSystemImpl.Row>> queues, final BufferedWriter[] writers,
                        final CountDownLatch _latch, final int fileSize, final String key,final String pathPrefix,
                        final boolean flag) throws IOException {
        for (int i = 0;i<writers.length;i++) {
            if (writers[i]==null) {
                writers[i] = writers[i] = Utils.createWriter(pathPrefix+i);
            }
        }

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
                            if (flag) {
                                indexProcessor.addBuyeridAndCreateTime(row.get("orderid").valueAsString(),
                                        row.get("createtime").valueAsString(),row.get("buyerid").valueAsString());
                                indexProcessor.addGoodidToOrderid(row.get("orderid").valueAsString(),
                                        row.get("goodid").valueAsString());
                            }
                            writers[index].write(row.get(key).valueAsString()+"&"+row.toString());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        _latch.countDown();
                    }
                }
            });
        }
    }

    public void sortData(final LinkedBlockingQueue<Object> queue, final CountDownLatch _latch, int fileSize,
                         final String prefixPath, final boolean flag) {
        for (int i = 0; i < fileSize; i++) {
            final int index = i;
            threads.execute(new Runnable() {
                @Override
                public void run() {
                    BufferedReader br = null;
                    BufferedWriter bw = null;
                    TreeMap<String,String> treeMap = null;
                    BplusTree bplusTree = null;

                    try {
                        br = Utils.createReader(prefixPath+index);
                        if (flag) {
                            bplusTree = new BplusTree(80);
                        } else {
                            treeMap = new TreeMap<>();
                        }
                        String line = br.readLine();
                        while (line!=null) {
                            String[] kv = line.split("&");
                            if (kv.length > 2) {
                                throw new RuntimeException("split regex error: "+line);
                            }
                            if (flag) {
                            bplusTree.insertOrUpdate(kv[0],kv[1]);

                            } else {
                                treeMap.put(kv[0],kv[1]);
                            }
                            line = br.readLine();
                        }

                        if (flag) {
                            // todo 使用flag
                            String path = prefixPath+"S"+index;
                            bw = Utils.createWriter(path);
                            bplusTree.getRoot().writeToDisk(0,bw);
                            ArrayList<String> indexs = new ArrayList<String>();
                            indexs.add(prefixPath+index);
                            for (Node node : bplusTree.getRoot().getChildren()) {
                                for (Node cnode : node.getChildren()) {
                                    indexs.add(cnode.toString());
                                }
                            }
                            queue.offer(indexs);
                        } else {
                            long pos = 0;
                            String path = prefixPath+"S"+index;
                            bw = Utils.createWriter(path);
                            Set<Map.Entry<String,String>> entrySet = treeMap.entrySet();
                            for (Map.Entry<String,String> entry : entrySet) {
                                char[] chars = entry.getValue().toCharArray();
                                String key = entry.getKey();
                                int length = entry.getValue().getBytes().length;
                                if (flag) {
                                    String[] keys = key.split("\t");
                                    key = keys[2];
                                }
                                queue.offer(new RecordIndex(path,key,pos,length));
                                pos += length;
                                bw.write(chars);
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            if (br!=null)
                                br.close();
                            if (bw!=null) {
                                bw.flush();
                                bw.close();
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        _latch.countDown();
                    }
                }
            });
        }
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
}
