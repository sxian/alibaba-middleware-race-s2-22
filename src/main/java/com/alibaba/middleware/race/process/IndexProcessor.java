package com.alibaba.middleware.race.process;

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

/**
 * Created by sxian.wang on 2016/7/21.
 */
public class IndexProcessor {

    private HashMap<String, TreeMap<Long,Long[]>> orderIndexs = QueryProcessor.filesIndex;
    private HashMap<String, Long[]> orderIndexsKeys = QueryProcessor.filesIndexKey;
    private final LinkedBlockingQueue<String[]> buyerid_create_order_queue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<String[]> goodid_orderid_queue = new LinkedBlockingQueue<>();

    private ExecutorService threads = Executors.newFixedThreadPool(5);
    private CountDownLatch latch = new CountDownLatch(5);

    private CountDownLatch hbLatch = new CountDownLatch(1);
    private CountDownLatch hgLatch = new CountDownLatch(1);
    private CountDownLatch orderLatch = new CountDownLatch(1);


    private long start;

    public IndexProcessor(long start) {
        this.start = start;
    }


    void init(LinkedBlockingQueue<String[]> hbIndexQueue, LinkedBlockingQueue<String[]> hgIndexQueue,
                     LinkedBlockingQueue<String[]> orderIndexQueue) throws IOException {
        new Thread(new ProcessOrderIndex(hbIndexQueue,RaceConfig.HB_FILE_SIZE,"o/hb",hbLatch,0)).start();
        new Thread(new ProcessOrderIndex(hgIndexQueue,RaceConfig.HG_FILE_SIZE,"o/hg",hgLatch,1)).start();
        new Thread(new ProcessOrderIndex(orderIndexQueue,RaceConfig.ORDER_FILE_SIZE,"o/i",orderLatch,2)).start();
    }

    private void buildHB() {
        threads.execute(new ProcessIndex(RaceConfig.DISK1+"o/hb", RaceConfig.HB_FILE_SIZE,latch));
        threads.execute(new ProcessIndex(RaceConfig.DISK2+"o/hb", RaceConfig.HB_FILE_SIZE,latch));
        threads.execute(new ProcessIndex(RaceConfig.DISK3+"o/hb", RaceConfig.HB_FILE_SIZE,latch));
    }

    private void buildHG() {
        threads.execute(new ProcessIndex(RaceConfig.DISK1+"o/hg", RaceConfig.HG_FILE_SIZE,latch));
        threads.execute(new ProcessIndex(RaceConfig.DISK2+"o/hg", RaceConfig.HG_FILE_SIZE,latch));
        threads.execute(new ProcessIndex(RaceConfig.DISK3+"o/hg", RaceConfig.HG_FILE_SIZE,latch));
    }

    private void buildOrderIndex() throws IOException {
        threads.execute(new ProcessIndex(RaceConfig.DISK1+"o/i", RaceConfig.ORDER_FILE_SIZE,latch));
        threads.execute(new ProcessIndex(RaceConfig.DISK2+"o/i", RaceConfig.ORDER_FILE_SIZE,latch));
        threads.execute(new ProcessIndex(RaceConfig.DISK3+"o/i", RaceConfig.ORDER_FILE_SIZE,latch));
    }

    void createBuyerIndex() throws IOException {
        threads.execute(new ProcessIndex(RaceConfig.DISK1+"b/i", RaceConfig.BUYER_FILE_SIZE,latch));
    }

    void createGoodsIndex() throws IOException {
        threads.execute(new ProcessIndex(RaceConfig.DISK2+"g/i", RaceConfig.GOODS_FILE_SIZE,latch));
    }

    private void createIndex(final LinkedBlockingQueue<Object> queue, final int flag) throws IOException {
        threads.execute(new Runnable() {
            @Override
            public void run() {
                long pos = 0;
                try {
                    int count = 0;
                    while (true) {
                            RecordIndex recordIndex = (RecordIndex) queue.take();
                            if (recordIndex.length == -1) {
                                break;
                            }
//                            char[] chars = recordIndex.toString().toCharArray();
//                            int length = chars.length;
                            QueryProcessor.addIndexCache(recordIndex,flag);
//                            pos += length;
                    }
                }catch (Exception e) {
                    e.printStackTrace();
                } finally {
//
//                    try {
//                        if (bw!=null){
//                            bw.flush();
//                            bw.close();
//                        }
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
                    latch.countDown();
                }
            }
        });
    }

    // 设置辅助索引
    public void setCache(BplusTree bplusTree,TreeMap<String,Long[]> tree,ArrayList<String> list) {
        for (Node node : bplusTree.getRoot().getChildren()) {
                for (Node _node : node.getChildren()) {
                    // node 内部节点的toString并不依赖于节点的length，但是叶子节点的依赖叶子节点的pos
                    // 所以在二次对叶子节点toString的时候，会偏移叶子节点的length长度个单位，这是因为writeToDisk方法
                    // 被调用后pos被更新为输出所有entries以及自身后的长度
                    String[] indexs = _node.toString().split(" ");
                    for (int j = 1;j<indexs.length;j++) {
                        try {
                            if (indexs[j].equals("\n")) {
                                continue;// 线上可以删了
                            }
                            String[] index = indexs[j].split(",");
                            tree.put(index[0],new Long[]{Long.valueOf(index[1]),
                                    Long.valueOf(index[2])});
                            list.add(index[0]);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
        }
    }

    void waitOver() throws InterruptedException {
        latch.await();
        threads.shutdown();
    }

    private class ProcessIndex implements Runnable {
        // 使用一个线程一个文件 -> 先一个线程处理所有文件试试
        private int fileNum;
        private String fileFold;
        private CountDownLatch latch;

        public ProcessIndex(String fileFold, int fileNum, CountDownLatch latch) {
            this.fileFold = fileFold;
            this.fileNum = fileNum;
            this.latch = latch;
        }

        @Override
        public void run() {
            for (int i = 0; i<fileNum; i++) {
                BufferedWriter bw = null;
                BufferedReader br = null;
                try {
                    br = Utils.createReader(fileFold+i);
                    bw = Utils.createWriter(fileFold+"S"+i);
                    String line = br.readLine();
                    BplusTree bpt = new BplusTree(60);
                    while (line!=null) {
                        String id = line.split(",")[0];
                        bpt.insertOrUpdate(id,line+" ");
                        line = br.readLine();
                    }
                    bpt.getRoot().writeToDisk(0,bw);
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (bw!=null) {
                            bw.flush();
                            bw.close();
                        }
                        if (br!=null) {
                            br.close();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            System.out.println(fileFold + " index process complete, now time: "+(System.currentTimeMillis() - start));
            latch.countDown();
        }
    }

    private class ProcessOrderIndex implements Runnable {
        int flag;
        int fileSize;
        String prefix;
        CountDownLatch latch;
        LinkedBlockingQueue<String[]> queue;

        BufferedWriter[][] writers;
        StringBuilder[][] builders;
        int[][] counters;

        public ProcessOrderIndex(LinkedBlockingQueue<String[]> queue, int fileSize, String prefix,
                                 CountDownLatch latch, int flag) throws IOException {
            this.queue = queue;
            this.fileSize = fileSize;
            this.prefix = prefix;
            this.latch = latch;
            this.flag = flag;
            init();
        }

        private void init() throws IOException {
            writers = new BufferedWriter[3][fileSize];
            builders = new StringBuilder[3][fileSize];
            counters = new int[3][fileSize];
            for (int i = 0;i<fileSize;i++) {
                counters[0][i] = 0;
                counters[1][i] = 0;
                counters[2][i] = 0;

                builders[0][i] = new StringBuilder();
                builders[1][i] = new StringBuilder();
                builders[2][i] = new StringBuilder();

                writers[0][i] = Utils.createWriter(RaceConfig.DISK1+prefix+i);
                writers[1][i] = Utils.createWriter(RaceConfig.DISK2+prefix+i);
                writers[2][i] = Utils.createWriter(RaceConfig.DISK3+prefix+i);
            }
        }

        @Override
        public void run() {
            try {
                while (true) {
                    String[] strings = queue.take();
                    if (strings.length == 0) {
                        break;
                    }
                    int disk = Math.abs(strings[0].hashCode()%3);
                    int file = Math.abs(strings[0].hashCode()%fileSize);
                    builders[disk][file].append(strings[0]).append(",").append(strings[1]).append("\n");

                    if (counters[disk][file]++ == 200) {
                        writers[disk][file].write(builders[disk][file].toString());
                        builders[disk][file].delete(0,builders[disk][file].length());
                        counters[disk][file] = 0;
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    for (int i = 0;i<fileSize;i++) {
                        writers[0][i].write(builders[0][i].toString());
                        writers[1][i].write(builders[1][i].toString());
                        writers[2][i].write(builders[2][i].toString());

                        writers[0][i].flush();
                        writers[1][i].flush();
                        writers[2][i].flush();

                        writers[0][i].close();
                        writers[1][i].close();
                        writers[2][i].close();
                    }
                    System.out.println(prefix +" process complete, now time: "+ (System.currentTimeMillis()-start));
                    latch.countDown();
                    switch (flag) {
                        case 0:
                            buildHB();
                            break;
                        case 1:
                            buildHG();
                            break;
                        case 2:
                            buildOrderIndex();
                            break;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


//    // 处理BuyeridAndCreateTime
//        new Thread(new Runnable() {
//        @Override
//        public void run() { // todo 线上测试实际数据的时候，生成的数据量会非常大，无法直接构架B+树,
//            // todo 所以要使用队列对这个数据切分. 另外有没有用户同一时间两个订单?
//            // todo 合并代码
//            String path = RaceConfig.STORE_PATH+"buyerid_create_order";
//            BufferedWriter bw = null;
//            try { // todo 有问题 -> 啥问题?
//                bw = Utils.createWriter(path);
//                HashMap<String,StringBuilder> map = new HashMap<>();
//                while (true) {
//                    String[] keys = buyerid_create_order_queue.take();
//                    if ("".equals(keys[0])&&"".equals(keys[1])&&"".equals(keys[2])) {
//                        break;
//                    }
//
//                    StringBuilder sb = map.get(keys[2]);
//                    if (sb == null) {
//                        sb = new StringBuilder(keys[0]+","+keys[1]+" ");
//                        map.put(keys[2],sb);
//                    } else {
//                        sb.append(keys[0]).append(",").append(keys[1]).append(" ");
//                    }
//                }
//                BplusTree bplusTree = new BplusTree(50);
//                for (Map.Entry<String,StringBuilder> entry : map.entrySet()) {
//                    bplusTree.insertOrUpdate(entry.getKey(),entry.getValue().toString());
//                }
//                bplusTree.getRoot().writeToDisk(0,bw);
//                setCache(bplusTree, QueryProcessor.buyerOrderFilesIndex,QueryProcessor.buyerOrderFilesIndexKey);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (IOException e) {
//                e.printStackTrace();
//            } finally {
//                if (bw!=null) {
//                    try {
//                        bw.flush();
//                        bw.close();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                }
//                latch.countDown();
//            }
//        }
//    }).start();
//
//    // GoodidToOrderid
//        new Thread(new Runnable() {
//        @Override
//        public void run() {
//            String path = RaceConfig.STORE_PATH+"goodid_orderid";
//            BufferedWriter bw = null;
//            try {
//                bw = Utils.createWriter(path);
//                HashMap<String,StringBuilder> map = new HashMap<>();
//
//                while (true) {
//                    String[] keys = goodid_orderid_queue.take();
//                    if ("".equals(keys[0])&&"".equals(keys[1])) {
//                        break;
//                    }
//
//                    StringBuilder sb = map.get(keys[1]);
//                    if (sb == null) {
//                        sb = new StringBuilder(keys[0]+" ");
//                        map.put(keys[1],sb);
//                    } else {
//                        sb.append(keys[0]).append(" ");
//                    }
//                }
//                BplusTree bplusTree = new BplusTree(50);
//                for (Map.Entry<String,StringBuilder> entry : map.entrySet()) {
//                    bplusTree.insertOrUpdate(entry.getKey(),entry.getValue().toString());
//                }
//                bplusTree.getRoot().writeToDisk(0,bw);
//                setCache(bplusTree, QueryProcessor.goodsOrderFilesIndex,QueryProcessor.goodsOrderFilesIndexKey);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (IOException e) {
//                e.printStackTrace();
//            } finally {
//                if (bw!=null) {
//                    try {
//                        bw.flush();
//                        bw.close();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                }
//                latch.countDown();
//            }
//        }
//    }).start();


//    new Thread(new Runnable() {
//        @Override
//        public void run() {
//            try {
//                BufferedWriter bw = Utils.createWriter(RaceConfig.STORE_PATH+"btreesIndex");
//                while (true) {
//                    ArrayList<String> list = (ArrayList<String>) queue.take();
//                    if (list.size() == 0) {
//                        break;
//                    }
//                    String path = list.get(0);
//                    StringBuilder sb = new StringBuilder("file "+path+"\n");
//                    TreeMap<Long,Long[]> treeMap = new TreeMap<>();
//                    for (int i = 1;i<list.size();i++) {
//                        String[] indexs = list.get(i).split(" ");
//                        for (int j = 1;j<indexs.length;j++) {
//                            try {
//                                if (indexs[j].equals("\n")) {
//                                    continue;// 线上可以删了
//                                }
//                                String[] index = indexs[j].split(",");
//                                long key = Long.valueOf(index[0]);
//                                long pos = Long.valueOf(index[1]);
//                                long length = Long.valueOf(index[2]);
//                                treeMap.put(key,new Long[]{pos,length});
//                            } catch (Exception e) {
//                                e.printStackTrace();
//                            }
//                        }
//                        sb.append(list.get(i));
//                    }
//                    bw.write(sb.toString().toCharArray());
//                    orderIndexs.put(path,treeMap);
//                    Object[] objects =  treeMap.keySet().toArray();
//                    Long[] keys = new Long[objects.length];
//                    for (int i = 0;i<objects.length;i++) {
//                        keys[i]= (Long) objects[i];
//                    }
//                    orderIndexsKeys.put(path, keys);
//                }
//                bw.flush();
//                bw.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } finally {
//                latch.countDown();
//            }
//        }
//    }).start();



}
