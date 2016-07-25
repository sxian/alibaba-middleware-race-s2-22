package com.alibaba.middleware.race.process;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.datastruct.BplusTree;
import com.alibaba.middleware.race.datastruct.Node;
import com.alibaba.middleware.race.datastruct.RecordIndex;
import com.alibaba.middleware.race.util.Utils;

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
    // todo 修改
    private String indexStorePath = RaceConfig.STORE_PATH+"index/";

    private HashMap<String, TreeMap<Long,Long[]>> orderIndexs = QueryProcessor.filesIndex;
    private HashMap<String, Long[]> orderIndexsKeys = QueryProcessor.filesIndexKey;
    private final LinkedBlockingQueue<String[]> buyerid_create_order_queue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<String[]> goodid_orderid_queue = new LinkedBlockingQueue<>();

    private ExecutorService threads = Executors.newCachedThreadPool();
    private CountDownLatch latch = new CountDownLatch(5);

    public void init() {
        // 处理BuyeridAndCreateTime
        new Thread(new Runnable() {
            @Override
            public void run() { // todo 线上测试实际数据的时候，生成的数据量会非常大，无法直接构架B+树,
                                     // todo 所以要使用队列对这个数据切分. 另外有没有用户同一时间两个订单?
                                     // todo 合并代码
                String path = RaceConfig.STORE_PATH+"buyerid_create_order";
                BufferedWriter bw = null;
                try { // todo 有问题 -> 啥问题?
                    bw = Utils.createWriter(path);
                    HashMap<String,StringBuilder> map = new HashMap<>();
                    while (true) {
                        String[] keys = buyerid_create_order_queue.take();
                        if ("".equals(keys[0])&&"".equals(keys[1])&&"".equals(keys[2])) {
                            break;
                        }

                        StringBuilder sb = map.get(keys[2]);
                        if (sb == null) {
                            sb = new StringBuilder(keys[0]+","+keys[1]+" ");
                            map.put(keys[2],sb);
                        } else {
                            sb.append(keys[0]).append(",").append(keys[1]).append(" ");
                        }
                    }
                    BplusTree bplusTree = new BplusTree(50);
                    for (Map.Entry<String,StringBuilder> entry : map.entrySet()) {
                        bplusTree.insertOrUpdate(entry.getKey(),entry.getValue().toString());
                    }
                    bplusTree.getRoot().writeToDisk(0,bw);
                    setCache(bplusTree, QueryProcessor.buyerOrderFilesIndex,QueryProcessor.buyerOrderFilesIndexKey);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if (bw!=null) {
                        try {
                            bw.flush();
                            bw.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    latch.countDown();
                }
            }
        }).start();

        // GoodidToOrderid
        new Thread(new Runnable() {
            @Override
            public void run() {
                String path = RaceConfig.STORE_PATH+"goodid_orderid";
                BufferedWriter bw = null;
                try {
                    bw = Utils.createWriter(path);
                    HashMap<String,StringBuilder> map = new HashMap<>();

                    while (true) {
                        String[] keys = goodid_orderid_queue.take();
                        if ("".equals(keys[0])&&"".equals(keys[1])) {
                            break;
                        }

                        StringBuilder sb = map.get(keys[1]);
                        if (sb == null) {
                            sb = new StringBuilder(keys[0]+" ");
                            map.put(keys[1],sb);
                        } else {
                            sb.append(keys[0]).append(" ");
                        }
                    }
                    BplusTree bplusTree = new BplusTree(50);
                    for (Map.Entry<String,StringBuilder> entry : map.entrySet()) {
                        bplusTree.insertOrUpdate(entry.getKey(),entry.getValue().toString());
                    }
                    bplusTree.getRoot().writeToDisk(0,bw);
                    setCache(bplusTree, QueryProcessor.goodsOrderFilesIndex,QueryProcessor.goodsOrderFilesIndexKey);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if (bw!=null) {
                        try {
                            bw.flush();
                            bw.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    latch.countDown();
                }
            }
        }).start();
    }

    public void addBuyeridAndCreateTime(String orderid, String createtime, String buyerid) {
        buyerid_create_order_queue.offer(new String[]{orderid,createtime,buyerid});
    }

    public void addGoodidToOrderid(String orderid, String goodsid) {
        goodid_orderid_queue.offer(new String[]{orderid, goodsid});
    }

    public void createOrderIndex(final LinkedBlockingQueue<Object> queue) throws IOException { // 结束条件 arraylist = 0
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    BufferedWriter bw = Utils.createWriter(RaceConfig.STORE_PATH+"btreesIndex");
                    while (true) {
                        ArrayList<String> list = (ArrayList<String>) queue.take();
                        if (list.size() == 0) {
                            break;
                        }
                        String path = list.get(0);
                        StringBuilder sb = new StringBuilder("file "+path+"\n");
                        TreeMap<Long,Long[]> treeMap = new TreeMap<>();
                        for (int i = 1;i<list.size();i++) {
                            String[] indexs = list.get(i).split(" ");
                            for (int j = 1;j<indexs.length;j++) {
                                try {
                                    if (indexs[j].equals("\n")) {
                                        continue;// 线上可以删了
                                    }
                                    String[] index = indexs[j].split(",");
                                    long key = Long.valueOf(index[0]);
                                    long pos = Long.valueOf(index[1]);
                                    long length = Long.valueOf(index[2]);
                                    treeMap.put(key,new Long[]{pos,length});
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                            sb.append(list.get(i));
                        }
                        bw.write(sb.toString().toCharArray());
                        orderIndexs.put(path,treeMap);
                        Object[] objects =  treeMap.keySet().toArray();
                        Long[] keys = new Long[objects.length];
                        for (int i = 0;i<objects.length;i++) {
                            keys[i]= (Long) objects[i];
                        }
                        orderIndexsKeys.put(path, keys);
                    }
                    bw.flush();
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }
        }).start();
    }

    // buyer good 还是使用原来的索引
    public void createBuyerIndex(LinkedBlockingQueue<Object> queue) throws IOException {
        createIndex(queue, 0);
//        createIndex(queue, indexStorePath+"buyerIndex_0");
    }

    public void createGoodsIndex(LinkedBlockingQueue<Object> queue) throws IOException {
        createIndex(queue, 1);
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

    public static BplusTree buildTree(List<String> files) throws IOException {
        BplusTree tree = new BplusTree(60);
        return tree;
    }

    public static HashMap<String,RecordIndex> buildIndexMap(List<String> files) throws IOException {
        HashMap<String,RecordIndex> map = new HashMap<>();
        return map;
    }

    public void waitOver() throws InterruptedException {
        latch.await();
        threads.shutdown();
    }
}
