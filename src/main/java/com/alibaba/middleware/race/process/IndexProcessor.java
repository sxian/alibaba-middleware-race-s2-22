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

    // todo 只需要将构建好的树的上层节点信息放到这里面，这一块的工作就先结束了
    private final HashMap<String, TreeMap<String,int[]>> filesIndexs = QueryProcessor.filesIndex;
    private final HashMap<String, String[]> filesIndexsKeys = QueryProcessor.filesIndexKey;

    private ExecutorService threads = Executors.newFixedThreadPool(5);
    private CountDownLatch latch = new CountDownLatch(11);

    // 貌似没啥卵用
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
        System.out.println("start build hb index, now time: " + (System.currentTimeMillis() - start));
        threads.execute(new ProcessAssistIndex(RaceConfig.DISK1+"o/hb", RaceConfig.HB_FILE_SIZE,latch,true));
        threads.execute(new ProcessAssistIndex(RaceConfig.DISK2+"o/hb", RaceConfig.HB_FILE_SIZE,latch,true));
        threads.execute(new ProcessAssistIndex(RaceConfig.DISK3+"o/hb", RaceConfig.HB_FILE_SIZE,latch,true));
    }

    private void buildHG() {
        System.out.println("start build hg index, now time: " + (System.currentTimeMillis() - start));
        threads.execute(new ProcessAssistIndex(RaceConfig.DISK1+"o/hg", RaceConfig.HG_FILE_SIZE,latch,false));
        threads.execute(new ProcessAssistIndex(RaceConfig.DISK2+"o/hg", RaceConfig.HG_FILE_SIZE,latch,false));
        threads.execute(new ProcessAssistIndex(RaceConfig.DISK3+"o/hg", RaceConfig.HG_FILE_SIZE,latch,false));
    }

    private void buildOrderIndex() throws IOException {
        System.out.println("start build order index, now time: " + (System.currentTimeMillis() - start));
        threads.execute(new ProcessIndex(RaceConfig.DISK1+"o/i", RaceConfig.ORDER_FILE_SIZE,latch));
        threads.execute(new ProcessIndex(RaceConfig.DISK2+"o/i", RaceConfig.ORDER_FILE_SIZE,latch));
        threads.execute(new ProcessIndex(RaceConfig.DISK3+"o/i", RaceConfig.ORDER_FILE_SIZE,latch));
    }

    void createBuyerIndex() throws IOException {
        System.out.println("start build buyer index, now time: " + (System.currentTimeMillis() - start));
        threads.execute(new ProcessIndex(RaceConfig.DISK1+"b/i", RaceConfig.BUYER_FILE_SIZE,latch));
    }

    void createGoodsIndex() throws IOException {
        System.out.println("start build goods index, now time: " + (System.currentTimeMillis() - start));
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
    public void setCache(BplusTree bplusTree,TreeMap<String,int[]> tree,ArrayList<String> list, String file) {
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
                            tree.put(index[0],new int[]{Integer.valueOf(index[1]),
                                    Integer.valueOf(index[2])});
                            list.add(index[0]);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
        }
        filesIndexs.put(file, tree);
        filesIndexsKeys.put(file, (String[]) list.toArray());
    }

    void waitOver() throws InterruptedException {
        latch.await();
        threads.shutdown();
    }

    private class ProcessIndex implements Runnable {
        // 使用一个线程一个文件 -> 先一个线程处理所有文件试试
        protected int fileNum;
        protected String fileFold;
        protected CountDownLatch latch;

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
                    System.out.println("*** build assist index: "+fileFold+i+", free mermory: "
                            +Runtime.getRuntime().freeMemory()+", max memory: "+Runtime.getRuntime().maxMemory()+" ***");
                    String line = br.readLine();
                    BplusTree bpt = new BplusTree(60);
                    while (line!=null) {
                        String id = line.split(",")[0];
                        bpt.insertOrUpdate(id,line+" ");
                        line = br.readLine();
                    }
                    bpt.getRoot().writeToDisk(0,bw);
                    bpt = null;
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
            System.out.println(fileFold + " index sort complete, now time: "+(System.currentTimeMillis() - start));
            latch.countDown();
        }
    }

    private class ProcessAssistIndex extends ProcessIndex {

        boolean flag;//0 hb, 1hg

        public ProcessAssistIndex(String fileFold, int fileNum, CountDownLatch latch,boolean flag) {
            super(fileFold, fileNum, latch);
            this.flag = flag;
        }

        public void run() {
            for (int i = 0; i<fileNum; i++) {
                BufferedWriter bw = null;
                BufferedReader br = null;
                try {
                    br = Utils.createReader(fileFold+i);
                    bw = Utils.createWriter(fileFold+"S"+i);
                    String line = br.readLine();
                    HashMap<String,StringBuilder> map = new HashMap<>();
                    System.out.println("*** build assist index: "+fileFold+i+", free mermory: "
                            +Runtime.getRuntime().freeMemory()+", max memory: "+Runtime.getRuntime().maxMemory()+" ***");
                    while (line!=null) {
                        String[] values = line.split(",");
                        StringBuilder sb = map.get(values[0]);
                        if (sb == null) {
                            sb = new StringBuilder();
                            map.put(values[0],sb);
                        }
                        if (flag) {
                            sb.append(values[1]).append(",").append(values[2]).append(" ");
                        } else {
                            sb.append(values[1]).append(" ");
                        }
                        line = br.readLine();
                    }
                    BplusTree bpt = new BplusTree(60);
                    for (Map.Entry<String, StringBuilder> entry : map.entrySet()) {
                        bpt.insertOrUpdate(entry.getKey(),entry.getValue().toString());
                    }
                    bpt.getRoot().writeToDisk(0,bw);
                    map = null;
                    bpt = null;
//                    for (Node node : bpt.getRoot().getChildren()) {
//                        if (node.getChildren()!=null) {
//                            for (Node _node : node.getChildren()) {
//
//                            }
//                        }
//                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (br!=null) {
                            br.close();
                        }
                        if (bw!=null) {
                            bw.flush();
                            bw.close();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            System.out.println(fileFold + " index sort complete, now time: "+(System.currentTimeMillis() - start));
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
}
