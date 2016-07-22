package com.alibaba.middleware.race.process;

import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.OrderSystemImpl.Row;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.datastruct.BplusTree;
import com.alibaba.middleware.race.datastruct.RecordIndex;
import com.alibaba.middleware.race.util.Utils;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by sxian.wang on 2016/7/21.
 */
public class IndexProcessor {
    //todo 索引文件中，文件的path占了很大1块空间，必须优化掉！ -> 如果在磁盘没有办法优化，装入内存的时候优化
    public ArrayList<LinkedBlockingQueue<RecordIndex>> orderIndexQueues;
    public ArrayList<LinkedBlockingQueue<RecordIndex>> buyerIndexQueues;
    public ArrayList<LinkedBlockingQueue<RecordIndex>> goodsIndexQueues;

    // todo 修改
    private String indexStorePath = RaceConfig.STORE_PATH+"index/";

    private ExecutorService threads = Executors.newCachedThreadPool();

    private CountDownLatch latch = new CountDownLatch(5);

    public void createOrderIndex(ArrayList<LinkedBlockingQueue<RecordIndex>> indexQueues) throws IOException {
        orderIndexQueues = indexQueues;
        int count = 0;
        for (final LinkedBlockingQueue<RecordIndex> queue : indexQueues) {
            createIndex(queue, Utils.createWriter(indexStorePath+"orderIndex_"+count++));
        }
    }

    public void createBuyerIndex(final ArrayList<LinkedBlockingQueue<RecordIndex>> indexQueues) throws IOException {
        buyerIndexQueues = indexQueues;
        int count = 0;
        for (final LinkedBlockingQueue<RecordIndex> queue : indexQueues) {
            createIndex(queue, Utils.createWriter(indexStorePath+"buyerIndex_"+count++));
        }
    }

    public void createGoodsIndex(ArrayList<LinkedBlockingQueue<RecordIndex>> indexQueues) throws IOException {
        goodsIndexQueues = indexQueues;
        int count = 0;
        for (final LinkedBlockingQueue<RecordIndex> queue : indexQueues) {
            createIndex(queue, Utils.createWriter(indexStorePath+"goodsIndex_"+count++));
        }
    }

    private void createIndex(final LinkedBlockingQueue<RecordIndex> queue, final BufferedWriter bw) {
        threads.execute(new Runnable() {
            @Override
            public void run() {
                long pos = 0;
                try {
                    while (true) {
                            RecordIndex recordIndex = queue.take();
                            if (recordIndex.length == -1) {
                                break;
                            }
                            char[] chars = recordIndex.toString().toCharArray();
                            String key = recordIndex.key;
                            int length = chars.length;

                            bw.write(chars);
                            pos += length;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (bw!=null){
                            bw.flush();
                            bw.close();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    latch.countDown();
                }
            }
        });
    }


    public static BplusTree buildTree(List<String> files) throws IOException {
        BplusTree tree = new BplusTree(60);
        for (String file : files) {
            BufferedReader br = Utils.createReader(file);
            String line = br.readLine();
            while (line!=null) {
                Row row = OrderSystemImpl.createRow(line);
                tree.insertOrUpdate(row.get("orderid").valueAsString(),row);
                line = br.readLine();
                int a = 1;
            }
            br.close();
        }
        return tree;
    }

    public static HashMap<String,RecordIndex> buildIndexMap(List<String> files) throws IOException {
        HashMap<String,RecordIndex> map = new HashMap<>();
        for (String file : files) {
            BufferedReader br = Utils.createReader(file);
            String line = br.readLine();
            while (line!=null) {
                RecordIndex recordIndex = new RecordIndex(line);
                map.put(recordIndex.key,recordIndex);
                line = br.readLine();
            }
            br.close();
        }
        return map;
    }
    public void waitOver() throws InterruptedException {
        latch.await();
        threads.shutdown();
    }
}
