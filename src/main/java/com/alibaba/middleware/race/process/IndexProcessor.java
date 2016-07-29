package com.alibaba.middleware.race.process;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.datastruct.*;
import com.alibaba.middleware.race.util.Utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by sxian.wang on 2016/7/21.
 */
public class IndexProcessor {

    private static final float M = 1024*1024;

    private final ConcurrentHashMap<String, TreeMap<String,int[]>> filesIndexs = QueryProcessor.filesIndex;
    private final ConcurrentHashMap<String, ArrayList<String>> filesIndexsKeys = QueryProcessor.filesIndexKey;

    private final ConcurrentHashMap<String,int[][]> map = QueryProcessor.indexMap;

    // todo 相同磁盘的放到一个队列
    private final LinkedBlockingQueue<Index> indexQueue1 = new LinkedBlockingQueue<>(2);
    private final LinkedBlockingQueue<Index> indexQueue2 = new LinkedBlockingQueue<>(2);
    private final LinkedBlockingQueue<Index> indexQueue3 = new LinkedBlockingQueue<>(2);


    private ExecutorService threads = Executors.newFixedThreadPool(6);
    private CountDownLatch latch = new CountDownLatch(5);
    private CountDownLatch _latch = new CountDownLatch(5);

    private CountDownLatch orderIndexLatch = new CountDownLatch(3);
    private CountDownLatch finalLatch = new CountDownLatch(3);

    private long start;

    public IndexProcessor(long start) {
        this.start = start;
    }

    void init(LinkedBlockingQueue<String[]> hbIndexQueue, LinkedBlockingQueue<String[]> hgIndexQueue,
                     LinkedBlockingQueue<String[]> orderIndexQueue) throws IOException {
        final LinkedBlockingQueue<String>[] sortIndexQueues = new LinkedBlockingQueue[9];
        for (int i = 0;i<9;i++) {
            sortIndexQueues[i] = new LinkedBlockingQueue<>(5000);
        }
        new Thread(new ProcessOrderIndex(hbIndexQueue,RaceConfig.HB_FILE_SIZE,"o/hb",orderIndexLatch,0)).start();
        new Thread(new ProcessOrderIndex(hgIndexQueue,RaceConfig.HG_FILE_SIZE,"o/hg",orderIndexLatch,1)).start();
        new Thread(new ProcessOrderIndex(orderIndexQueue,RaceConfig.ORDER_FILE_SIZE,"o/i",orderIndexLatch,2)).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    orderIndexLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                threads.execute(new ProcessIndex(RaceConfig.DISK1+"o/i", RaceConfig.ORDER_FILE_SIZE,
                        latch,sortIndexQueues[0]));
                threads.execute(new ProcessIndex(RaceConfig.DISK2+"o/i", RaceConfig.ORDER_FILE_SIZE,
                        latch,sortIndexQueues[1]));
                threads.execute(new ProcessIndex(RaceConfig.DISK3+"o/i", RaceConfig.ORDER_FILE_SIZE,
                        latch,sortIndexQueues[2]));

                threads.execute(new BuildHashIndex(sortIndexQueues[0],RaceConfig.DISK1+"o/iS",_latch,
                        indexQueue1,RaceConfig.ORDER_FILE_SIZE));
                threads.execute(new BuildHashIndex(sortIndexQueues[1],RaceConfig.DISK2+"o/iS",_latch,
                        indexQueue2,RaceConfig.ORDER_FILE_SIZE));
                threads.execute(new BuildHashIndex(sortIndexQueues[2],RaceConfig.DISK3+"o/iS",_latch,
                        indexQueue3,RaceConfig.ORDER_FILE_SIZE));

//                System.out.println("start build hb index, now time: " + (System.currentTimeMillis() - start));
//                threads.execute(new ProcessIndex(RaceConfig.DISK1+"o/hb", RaceConfig.HB_FILE_SIZE,latch,
//                        sortIndexQueues[3]));
//                threads.execute(new ProcessIndex(RaceConfig.DISK2+"o/hb", RaceConfig.HB_FILE_SIZE,latch,
//                        sortIndexQueues[4]));
//                threads.execute(new ProcessIndex(RaceConfig.DISK3+"o/hb", RaceConfig.HB_FILE_SIZE,latch,
//                        sortIndexQueues[5]));
//
//                threads.execute(new BuildAssistHashIndex(sortIndexQueues[3],RaceConfig.DISK1+"o/hbS",_latch,
//                        1,RaceConfig.HB_FILE_SIZE));
//                threads.execute(new BuildAssistHashIndex(sortIndexQueues[4],RaceConfig.DISK2+"o/hbS",_latch,
//                        2,RaceConfig.HB_FILE_SIZE));
//                threads.execute(new BuildAssistHashIndex(sortIndexQueues[5],RaceConfig.DISK3+"o/hbS",_latch,
//                        3,RaceConfig.HB_FILE_SIZE));
//
//                System.out.println("start build hg index, now time: " + (System.currentTimeMillis() - start));
//                threads.execute(new ProcessIndex(RaceConfig.DISK1+"o/hg", RaceConfig.HG_FILE_SIZE,latch,
//                        sortIndexQueues[6]));
//                threads.execute(new ProcessIndex(RaceConfig.DISK2+"o/hg", RaceConfig.HG_FILE_SIZE,latch,
//                        sortIndexQueues[7]));
//                threads.execute(new ProcessIndex(RaceConfig.DISK3+"o/hg", RaceConfig.HG_FILE_SIZE,latch,
//                        sortIndexQueues[8]));
//
//                threads.execute(new BuildAssistHashIndex(sortIndexQueues[6],RaceConfig.DISK1+"o/hgS",_latch,
//                        1,RaceConfig.HG_FILE_SIZE));
//                threads.execute(new BuildAssistHashIndex(sortIndexQueues[7],RaceConfig.DISK2+"o/hgS",_latch,
//                        2,RaceConfig.HG_FILE_SIZE));
//                threads.execute(new BuildAssistHashIndex(sortIndexQueues[8],RaceConfig.DISK3+"o/hgS",_latch,
//                        3,RaceConfig.HG_FILE_SIZE));


                writeIndexToDisk(indexQueue1);
                writeIndexToDisk(indexQueue2);
                writeIndexToDisk(indexQueue3);

            }
        }).start();
    }

    public void writeIndexToDisk(final LinkedBlockingQueue<Index> queue) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        Index index = queue.take();
                        if (index.flag) {
                            break;
                        }
                        map.put(index.FILE_PATH, index.writeToDisk());
                        System.out.println("!!! "+index.FILE_PATH+" complete, now time: "+(System.currentTimeMillis()-start));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    finalLatch.countDown();
                }
            }
        }).start();
    }
    private void buildHB() {
        System.out.println("start build hb index, now time: " + (System.currentTimeMillis() - start));
//        threads.execute(new ProcessAssistIndex(RaceConfig.DISK1+"o/hb", RaceConfig.HB_FILE_SIZE,latch,true));
//        threads.execute(new ProcessAssistIndex(RaceConfig.DISK2+"o/hb", RaceConfig.HB_FILE_SIZE,latch,true));
//        threads.execute(new ProcessAssistIndex(RaceConfig.DISK3+"o/hb", RaceConfig.HB_FILE_SIZE,latch,true));
    }

    private void buildHG() {
        System.out.println("start build hg index, now time: " + (System.currentTimeMillis() - start));
//        threads.execute(new ProcessAssistIndex(RaceConfig.DISK2+"o/hg", RaceConfig.HG_FILE_SIZE,latch,false));
//        threads.execute(new ProcessAssistIndex(RaceConfig.DISK3+"o/hg", RaceConfig.HG_FILE_SIZE,latch,false));
//        threads.execute(new ProcessAssistIndex(RaceConfig.DISK1+"o/hg", RaceConfig.HG_FILE_SIZE,latch,false));
    }

    private void buildOrderIndex() throws IOException {
        System.out.println("start build order index, now time: " + (System.currentTimeMillis() - start));
//        threads.execute(new ProcessIndex(RaceConfig.DISK3+"o/i", RaceConfig.ORDER_FILE_SIZE,latch));
//        threads.execute(new ProcessIndex(RaceConfig.DISK1+"o/i", RaceConfig.ORDER_FILE_SIZE,latch));
//        threads.execute(new ProcessIndex(RaceConfig.DISK2+"o/i", RaceConfig.ORDER_FILE_SIZE,latch));
    }

    void createBuyerIndex() throws IOException {
        System.out.println("start build buyer index, now time: " + (System.currentTimeMillis() - start));
        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>(5000);
        threads.execute(new ProcessIndex(RaceConfig.DISK1+"b/i", RaceConfig.BUYER_FILE_SIZE,latch,queue));
        threads.execute(new BuildHashIndex(queue,RaceConfig.DISK1+"b/iS",_latch,indexQueue1,RaceConfig.BUYER_FILE_SIZE));
    }

    void createGoodsIndex() throws IOException {
        System.out.println("start build goods index, now time: " + (System.currentTimeMillis() - start));
        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>(5000);
        threads.execute(new ProcessIndex(RaceConfig.DISK2+"g/i", RaceConfig.GOODS_FILE_SIZE,latch,queue));
        threads.execute(new BuildHashIndex(queue,RaceConfig.DISK2+"g/iS",_latch,indexQueue2,RaceConfig.GOODS_FILE_SIZE));
    }

    // 设置辅助索引
    public void setCache(BplusTree bplusTree, String file) {
        TreeMap<String,int[]> tree = new TreeMap<>();
        ArrayList<String> list = new ArrayList<>();
        Node a = bplusTree.getHead();
        int num = 0;
        while (a!=null) {
            num += a.getEntries().size();
            a = a.getNext();
        }
        System.out.println(file+" actually num is: "+ num);
        try {
            for (Node node : bplusTree.getRoot().getChildren()) {
                if (node.getChildren() != null) {
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
                } else {
                    String[] indexs = node.toString().split(" ");
                    for (int j = 1;j<indexs.length;j++) {
                        try {
                            if (indexs[j].equals("\n")) {
                                continue;// 线上可以删了
                            }
                            String[] index = indexs[j].split(",");
                            list.add(index[0]);
                            tree.put(index[0],new int[]{Integer.valueOf(index[1]),
                                    Integer.valueOf(index[2])});
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            filesIndexs.put(file, tree);
            Collections.sort(list);
            filesIndexsKeys.put(file, list);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void waitOver() throws InterruptedException {
        latch.await();
        _latch.await();
        indexQueue1.offer(new Index(true),600,TimeUnit.SECONDS);
        indexQueue2.offer(new Index(true),600,TimeUnit.SECONDS);
        indexQueue3.offer(new Index(true),600,TimeUnit.SECONDS);
        finalLatch.countDown();
        threads.shutdown();
    }

    private class ProcessIndex implements Runnable {
        // 使用一个线程一个文件 -> 先一个线程处理所有文件试试
        protected int fileNum;
        protected String fileFold;
        protected CountDownLatch latch;
        LinkedBlockingQueue<String> queue;

        public ProcessIndex(String fileFold, int fileNum, CountDownLatch latch,LinkedBlockingQueue<String> queue) {
            this.fileFold = fileFold;
            this.fileNum = fileNum;
            this.latch = latch;
            this.queue = queue;
        }

        @Override
        public void run() {
            for (int i = 0; i<fileNum; i++) {
                BufferedReader br = null;
                try {
                    br = Utils.createReader(fileFold+i);
                    String line = br.readLine();
                    while (line!=null) {
                        queue.offer(line,60,TimeUnit.SECONDS);
                        line = br.readLine();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        queue.offer(String.valueOf(i),60,TimeUnit.SECONDS);
                        if (br!=null) {
                            br.close();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            System.out.println(fileFold + " index sort complete, now time: "+(System.currentTimeMillis() - start));
            latch.countDown();
        }
    }

    private class BuildAssistHashIndex extends BuildHashIndex {

        public BuildAssistHashIndex(LinkedBlockingQueue<String> queue, String path, CountDownLatch latch,
                                    LinkedBlockingQueue<Index> diskqueue, int fileSize) {
            super(queue,path,latch,diskqueue,fileSize);
        }

        @Override
        public void run() {
            try {
                Index index = new AssistIndex(path+0);
                while (true) {
                    String line = queue.take();
                    if (line.length()<3) {
                        int num = Integer.valueOf(line)+1;
                        if (num==fileSize) {
                            diskqueue.offer(index,600,TimeUnit.SECONDS);
                            break;
                        }
                        diskqueue.offer(index,600,TimeUnit.SECONDS);
                        System.out.println("+++ "+index.FILE_PATH+" +++");
                        index = new AssistIndex(path+num);
                        continue;
                    }
                    index.add(line.substring(0,line.indexOf(",")),line);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                latch.countDown();
            }
        }

    }

    private class BuildHashIndex implements Runnable {
        LinkedBlockingQueue<Index> diskqueue;
        int fileSize;
        String path;
        CountDownLatch latch;
        LinkedBlockingQueue<String> queue;

        public BuildHashIndex(LinkedBlockingQueue<String> queue, String path, CountDownLatch latch,
                              LinkedBlockingQueue<Index> diskqueue, int fileSize) {
            this.queue = queue;
            this.path = path;
            this.latch = latch;
            this.diskqueue = diskqueue;
            this.fileSize = fileSize;
        }

        @Override
        public void run() {
            try {
                Index index = new Index(path+0);
                while (true) {
                    String line = queue.take();
                    if (line.length()<3) {
                        int num = Integer.valueOf(line)+1;
                        if (num==fileSize) {
                            diskqueue.offer(index,600,TimeUnit.SECONDS);
                            break;
                        }
                        diskqueue.offer(index,600,TimeUnit.SECONDS);
                        System.out.println("+++ "+index.FILE_PATH+" +++");
                        index = new Index(path+num);
                        continue;
                    }
                    index.add(line.substring(0,line.indexOf(",")),line);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                latch.countDown();
            }
        }
    }

    private class BuildTree implements Runnable { // 处理order
        boolean flag;
        int fileSize;
        String path;
        CountDownLatch latch;
        LinkedBlockingQueue<String> queue;

        public BuildTree(LinkedBlockingQueue<String> queue, String path, CountDownLatch latch,boolean flag,
                         int fileSize) {
            this.queue = queue;
            this.path = path;
            this.latch = latch;
            this.flag = flag;
            this.fileSize = fileSize;
        }

        @Override
        public void run() {
            int i = 0;
            try {
                BplusTree bpt = new BplusTree(200);
                BufferedWriter bw = Utils.createWriter(path+0);
                int count = 0;
                while (true) {
                    String line = queue.take();
                    if (line.length()<3) {
                        int num = Integer.valueOf(line)+1;
                        if (num==fileSize) {
                            bpt.getRoot().writeToDisk(0,bw);
                            bw.flush();
                            bw.close();
//                            setCache(bpt, path+(num-1));
                            break;
                        }
                        System.out.println("file "+path+(num-1)+" rows: "+count);
                        count = 0;
                        bpt.getRoot().writeToDisk(0,bw);
                        bw.flush();
                        bw.close();
//                        setCache(bpt, path+(num-1));
                        bpt = new BplusTree(200);
                        bw = Utils.createWriter(path+ num);
                        continue;
                    }
                    count++;
                    i++;
                    if (flag) { // orderid
                        int flag0 = line.indexOf(",",10)+1;
                        int flag = line.indexOf(",",flag0);// path 的逗号的位置
                        bpt.insertOrUpdate(line.substring(0,9),new Object[] {line.substring(10,line.indexOf(",",10)),
                                line.substring(flag0,flag),line.substring(flag+1,line.length())});
                    } else {
                        int index1 = line.indexOf(',');
                        int index2 = line.lastIndexOf(',');
                        try {

                        bpt.insertOrUpdate(line.substring(0,index1),new Object[] {line.substring(index1+1,index2),
                                line.substring(index2+1,line.length())});
                        } catch (Exception e ) {
                            int a = 1;
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                System.out.println("process "+path+" row is: " + i);
                latch.countDown();
            }
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
                    writers = null;
                    builders = null;
                    counters = null;
                    latch.countDown();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
