package com.alibaba.middleware.race;

import com.alibaba.middleware.race.db.BuyerTable;
import com.alibaba.middleware.race.db.GoodsTable;
import com.alibaba.middleware.race.db.OrderTable;
import com.alibaba.middleware.race.process.FileProcessor;
import com.alibaba.middleware.race.process.IndexProcessor;
import com.alibaba.middleware.race.process.QueryProcessor;
import com.alibaba.middleware.race.util.Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 订单系统的demo实现，订单数据全部存放在内存中，用简单的方式实现数据存储和查询功能
 *
 * @author wangxiang@alibaba-inc.com
 *
 */
public class OrderSystemImpl implements OrderSystem {

    private static String booleanTrueValue = "true";
    private static String booleanFalseValue = "false";

    // 每个队列对应一个磁盘
    public LinkedBlockingQueue<String>[] orderQueues = new LinkedBlockingQueue[3];
    public LinkedBlockingQueue<String> buyerQueue = new LinkedBlockingQueue<>(150000);
    public LinkedBlockingQueue<String> goodsQueue = new LinkedBlockingQueue<>(150000);

    private FileProcessor fileProcessor;
    private IndexProcessor indexProcessor;

    private OrderTable orderTable;
    private BuyerTable buyerTable;
    private GoodsTable goodsTable;

    public static long start;
    public OrderSystemImpl() {
        fileProcessor = new FileProcessor(this);
    }

    public static class KV implements Comparable<KV>, KeyValue {
        String key;
        String rawValue;

        boolean isComparableLong = false;
        long longValue;

        private KV(String key, String rawValue) {
            this.key = key;
            this.rawValue = rawValue;
            if (key.equals("createtime") || key.equals("orderid")) {
                isComparableLong = true;
                longValue = Long.parseLong(rawValue);
            }
        }

        public String key() {
            return key;
        }

        public String valueAsString() {
            return rawValue;
        }

        public long valueAsLong() throws TypeException {
            try {
                return Long.parseLong(rawValue);
            } catch (NumberFormatException e) {
                throw new TypeException();
            }
        }

        public double valueAsDouble() throws TypeException {
            try {
                return Double.parseDouble(rawValue);
            } catch (NumberFormatException e) {
                throw new TypeException();
            }
        }

        public boolean valueAsBoolean() throws TypeException {
            if (this.rawValue.equals(booleanTrueValue)) {
                return true;
            }
            if (this.rawValue.equals(booleanFalseValue)) {
                return false;
            }
            throw new TypeException();
        }

        public int compareTo(KV o) {
            if (!this.key().equals(o.key())) {
                throw new RuntimeException("Cannot compare from different key");
            }
            if (isComparableLong) {
                return Long.compare(this.longValue, o.longValue);
            }
            return this.rawValue.compareTo(o.rawValue);
        }

        @Override
        public String toString() {
            return "[" + this.key + "]:" + this.rawValue;
        }
    }

    @SuppressWarnings("serial")
    public static class Row extends HashMap<String, KV> {
        Row() {
            super();
        }

        Row(KV kv) {
            super();
            this.put(kv.key(), kv);
        }

        KV getKV(String key) {
            KV kv = this.get(key);
            if (kv == null) {
                throw new RuntimeException(key + " is not exist");
            }
            return kv;
        }

        Row putKV(String key, String value) {
            KV kv = new KV(key, value);
            this.put(kv.key(), kv);
            return this;
        }

        Row putKV(String key, long value) {
            KV kv = new KV(key, Long.toString(value));
            this.put(kv.key(), kv);
            return this;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, KV> entry : entrySet()) {

                sb.append(entry.getKey()).append(':').append(entry.getValue().rawValue).append('\t');
            }
            return sb.append('\n').toString();
        }
    }

    private static class ResultImpl implements Result {
        private long orderid;
        private Row kvMap;

        private ResultImpl(long orderid, Row kv) {
            this.orderid = orderid;
            this.kvMap = kv;
        }

        private ResultImpl(long orderid, Row kv, Set<String> queryingKeys) {
            this.orderid = orderid;
            this.kvMap = new Row();
            filterKeys(kv, queryingKeys);
        }

        private ResultImpl(long orderid, Row kv1, Row kv2,Set<String> queryingKeys) {
            this.orderid = orderid;
            this.kvMap = new Row();
            filterKeys(kv1, queryingKeys);
            filterKeys(kv2, queryingKeys);
        }

        private void filterKeys(Row row, Set<String> queryingKeys) {
            if (queryingKeys.size() == 0) {
                return;
            }
            for (KV kv : row.values()) {
                if (queryingKeys == null || queryingKeys.contains(kv.key)) {
                    kvMap.put(kv.key(), kv);
                }
            }
        }

        static private ResultImpl createResultRow(Row orderData, Row buyerData,
                                                  Row goodData, Set<String> queryingKeys) {
            if (orderData == null || buyerData == null || goodData == null) {
                throw new RuntimeException("Bad data!");
            }
            Row allkv = new Row();

            long orderid;
            try {
                orderid = orderData.get("orderid").valueAsLong();
            } catch (TypeException e) {
                throw new RuntimeException("Bad data!");
            }

            if (queryingKeys!=null && queryingKeys.size()==0)
                return new ResultImpl(orderid, allkv);

            for (KV kv : orderData.values()) {
                if (queryingKeys == null || queryingKeys.contains(kv.key)) {
                    allkv.put(kv.key(), kv);
                }
            }
            for (KV kv : buyerData.values()) {
                if (queryingKeys == null || queryingKeys.contains(kv.key)) {
                    allkv.put(kv.key(), kv);
                }
            }
            for (KV kv : goodData.values()) {
                if (queryingKeys == null || queryingKeys.contains(kv.key)) {
                    allkv.put(kv.key(), kv);
                }
            }
            return new ResultImpl(orderid, allkv);
        }

        public KeyValue get(String key) {
            return this.kvMap.get(key);
        }

        public KeyValue[] getAll() {
            return kvMap.values().toArray(new KeyValue[0]);
        }

        public long orderId() {
            return orderid;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("orderid: " + orderid + " {");
            if (kvMap != null && !kvMap.isEmpty()) {
                for (KV kv : kvMap.values()) {
                    sb.append(kv.toString());
                    sb.append(",\n");
                }
            }
            sb.append('}');
            return sb.toString();
        }
    }

    private static class ComparableKeys implements Comparable<ComparableKeys> {
        List<String> orderingKeys;
        Row row;

        private ComparableKeys(List<String> orderingKeys, Row row) {
            if (orderingKeys == null || orderingKeys.size() == 0) {
                throw new RuntimeException("Bad ordering keys, there is a bug maybe");
            }
            this.orderingKeys = orderingKeys;
            this.row = row;
        }

        public int compareTo(ComparableKeys o) {
            if (this.orderingKeys.size() != o.orderingKeys.size()) {
                throw new RuntimeException("Bad ordering keys, there is a bug maybe");
            }
            for (String key : orderingKeys) {
                KV a = this.row.get(key);
                KV b = o.row.get(key);
                if (a == null || b == null) {
                    throw new RuntimeException("Bad input data: " + key);
                }
                int ret = a.compareTo(b);
                if (ret != 0) {
                    return ret;
                }
            }
            return 0;
        }
    }

    private void sendEndMsg(LinkedBlockingQueue<String>[] queues) throws InterruptedException {
        for (LinkedBlockingQueue<String> queue : queues) {
            queue.offer("",60,TimeUnit.SECONDS);
        }
    }

    public void construct(Collection<String> orderFiles,
                          Collection<String> buyerFiles, Collection<String> goodFiles,
                          Collection<String> storeFolders) throws IOException, InterruptedException {
        ArrayList<String> disk1 = new ArrayList<>();
        ArrayList<String> disk2 = new ArrayList<>();
        ArrayList<String> disk3 = new ArrayList<>();

        for (String storePath : storeFolders) {
            if (storePath.startsWith("/disk1")) {
                RaceConfig.DISK1 = storePath;
            } else if (storePath.startsWith("/disk2")) {
                RaceConfig.DISK2 = storePath;
            } else {
                RaceConfig.DISK3 = storePath;
            }
            new File(storePath+"o/").mkdirs();
            new File(storePath+"b/").mkdirs();
            new File(storePath+"g/").mkdirs();
        }
        for (String file : orderFiles) {
            if (file.startsWith("/disk1")) {
                disk1.add(file);
            } else if (file.startsWith("/disk2")) {
                disk2.add(file);
            } else {
                disk3.add(file);
            }
        }

        for (int i = 0;i<3;i++) { // todo 参数优化
            orderQueues[i] = new LinkedBlockingQueue<>(100000);
        }

        // 设置latch数目，确保所有数据都处理完
        final CountDownLatch orderLatch = new CountDownLatch(orderFiles.size());
        final CountDownLatch buyerLatch = new CountDownLatch(buyerFiles.size());
        final CountDownLatch goodsLatch = new CountDownLatch(goodFiles.size());

        this.start = System.currentTimeMillis();

        // 5个读取数据的线程
        new OrderDataFileHandler().handle(orderQueues[0], disk1, 4, orderLatch); //"(orderid|buyerid|goodid|createtime):([\\w|-]+)"
        new OrderDataFileHandler().handle(orderQueues[1], disk2, 4, orderLatch);
        new OrderDataFileHandler().handle(orderQueues[2], disk3, 4, orderLatch);

        new DataFileHandler().handle(buyerQueue, buyerFiles,1, buyerLatch);

        new DataFileHandler().handle(goodsQueue, goodFiles, 1, goodsLatch);

        indexProcessor = new IndexProcessor(start);
        fileProcessor.init(start, indexProcessor);
        indexProcessor = null;

        buyerLatch.await();
        buyerQueue.offer("",60,TimeUnit.SECONDS);
        buyerQueue = null;
        System.out.println("process buyer data, now time: " + (System.currentTimeMillis() - start));
        goodsLatch.await();
        goodsQueue.offer("",60,TimeUnit.SECONDS);
        goodsQueue = null;
        System.out.println("process goods data, now time: " + (System.currentTimeMillis() - start));
        orderLatch.await(); // 等待处理完所有文件
        sendEndMsg(orderQueues); // 发送结束信号
        orderQueues = null;
        System.out.println("process order data, now time: " + (System.currentTimeMillis() - start));
        fileProcessor.waitOver(); // 等待队列处理完毕
        fileProcessor = null;
        QueryProcessor.initFile();
        System.out.println("all data process complete, now time: " + (System.currentTimeMillis() - start));
        orderTable = new OrderTable();
        buyerTable = new BuyerTable();
        goodsTable = new GoodsTable();
        System.out.println("successfully processed!");
        System.out.println("free memory: " +Runtime.getRuntime().freeMemory()/(1024.0*1024.0));
        System.gc();
        System.out.println("free memory: " +Runtime.getRuntime().freeMemory()/(1024.0*1024.0));
    }

    public static Row createRow(String line) {
        String[] kvs = line.split("\t");
        Row kvMap = new Row();
        for (String rawkv : kvs) {
            int p = rawkv.indexOf(':');
            String key = rawkv.substring(0, p);
            String value = rawkv.substring(p + 1);
            if (key.length() == 0 || value.length() == 0) {
                throw new RuntimeException("Bad data:" + line);
            }
            KV kv = new KV(key, value);
            kvMap.put(kv.key(), kv);
        }
        return kvMap;
    }

    public static KV createKV(String key, String value) {
        return new KV(key,value);
    }

    @Override
    public Result queryOrder(long orderId, Collection<String> keys) {
        long start = System.currentTimeMillis();
        Row orderRow =  orderTable.selectRowById(String.valueOf(orderId)); //判断join不join很重要
        Row buyerRow = new Row();
        Row goodsRow = new Row();
        // todo 官方的借口demo改了一下，方便build result 看着改下
        if (orderRow  == null) // todo 测试substring 和StringTokenizer
            return null;
        HashSet<String> set = new HashSet<>(orderRow.keySet());
        if (keys == null) {
            buyerRow = buyerTable.selectRowById(orderRow.get("buyerid").valueAsString());
            goodsRow = goodsTable.selectRowById(orderRow.get("goodid").valueAsString());
            return ResultImpl.createResultRow(orderRow, buyerRow, goodsRow, null);
        } else {
            if (!set.containsAll(keys)) {
                buyerRow = buyerTable.selectRowById(orderRow.get("buyerid").valueAsString());
                set.addAll(buyerRow.keySet());
                if (!set.containsAll(keys)) {
                    goodsRow = goodsTable.selectRowById(orderRow.get("goodid").valueAsString());
                }
            }
        }
        return ResultImpl.createResultRow(orderRow, buyerRow, goodsRow, new HashSet<>(keys));
    }

    @Override
    public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime, String buyerid) {
        long start = System.currentTimeMillis();
        ArrayList<Result> results = new ArrayList<>();
        List<Row> list = orderTable.selectOrderIDByBuyerID(buyerid,startTime,endTime);
        for (int i = 0;i<list.size();i++ ) {
            Row buyerRow = buyerTable.selectRowById(list.get(i).get("buyerid").valueAsString());
            Row goodsRow = goodsTable.selectRowById(list.get(i).get("goodid").valueAsString());
            results.add(ResultImpl.createResultRow(list.get(i),buyerRow, goodsRow,null));
        }
        return results.iterator();
    }

    @Override
    public Iterator<Result> queryOrdersBySaler(String salerid, String goodid, Collection<String> keys) {
        long start = System.currentTimeMillis();

        ArrayList<Result> results = new ArrayList<>();
        Row goodsRow = goodsTable.selectRowById(goodid);
        if (goodsRow == null) {
            return results.iterator();
        }
        List<Row> _result = orderTable.selectOrderIDByGoodsID(goodid);
        if (keys!=null) {
            HashSet<String> set = new HashSet<>(goodsRow.keySet());
            boolean unjoin = set.containsAll(keys);
            try {
                for (int i = 0;i<_result.size();i++) {
                    if (unjoin) {
                        results.add(new ResultImpl(_result.get(i).get("orderid").valueAsLong(),goodsRow,new HashSet<>(keys)));
                    } else {
                        set.addAll(_result.get(i).keySet());
                    }
                }
                if (!unjoin) {
                    if (set.containsAll(keys)) {
                        for (int i = 0;i<_result.size();i++) {
                            results.add(new ResultImpl(_result.get(i).get("orderid").valueAsLong(),_result.get(i),goodsRow,new HashSet<>(keys)));
                        }
                    } else {
                        for (int i = 0;i<_result.size();i++) {
                            Row buyerRow = buyerTable.selectRowById(_result.get(i).get("buyerid").valueAsString());
                            results.add(ResultImpl.createResultRow(_result.get(i),buyerRow, goodsRow,new HashSet<>(keys)));
                        }
                    }
                }
            } catch (TypeException e) {
                e.printStackTrace();
            }
        } else {
            for (int i = 0;i<_result.size();i++) {
                Row buyerRow = buyerTable.selectRowById(_result.get(i).get("buyerid").valueAsString());
                results.add(ResultImpl.createResultRow(_result.get(i),goodsRow, buyerRow,null));
            }
        }
//        System.out.println("queryOrdersBySaler: "+(System.currentTimeMillis() -start));
        return results.iterator();
    }

    @Override
    public KeyValue sumOrdersByGood(String goodid, String key) {
        long start = System.currentTimeMillis();
        Set<String> set  = new HashSet<>();
        set.add(key);
        Iterator<Result> iterator =  queryOrdersBySaler("",goodid, set);
        List<String> _list = new ArrayList<>();
        _list.add(key);
        double sumDouble = 0;
        long sumLong = 0;
        boolean existKey = false;
        boolean existDouble = false;
        boolean existStr = false;
        if (!iterator.hasNext())
            return null;

        while (iterator.hasNext()) {
            Result result = iterator.next();
            KV kv = (KV) result.get(key);
            if (kv == null)
                continue;
            if (!existKey) {
                existKey = true;
            }
            try { // todo 改一下判断方法 intanceof或者contains(".")
                if (existDouble) {
                    double tmp = kv.valueAsDouble();
                    sumDouble += tmp;
                } else {
                    long tmp = kv.valueAsLong();
                    sumLong += tmp;
                }
            } catch (TypeException e) {
                if (!existDouble) { // 如果exitDoube为true, 上面肯定转型的是double，double转型失败必然为string
                    try {
                        double tmp = kv.valueAsDouble();
                        sumDouble = tmp + sumLong;
                        existDouble = true;
                        continue;
                    } catch (TypeException e1) {
                    }
                }
                existStr = true;
                break;
            }
        }

        if (existDouble) {
            return new KV(key,String.valueOf(sumDouble));
        }
        return (!existKey || existStr) ? null : new KV(key,String.valueOf(sumLong));
    }

    private class DataFileHandler {
        LinkedBlockingQueue<String> queue;
        void emit(String str) throws InterruptedException {
            queue.offer(str,60,TimeUnit.SECONDS);
        }

        void handle(LinkedBlockingQueue<String> queue, Collection<String> files, final int keyNum,
                    final CountDownLatch latch) {
            this.queue = queue;
            new Thread(new FileHandler(keyNum,files,latch,this)).start();
        }
    }

    private class OrderDataFileHandler extends DataFileHandler{
        @Override
        void emit(String row) throws InterruptedException {
            int goodid_of = row.indexOf("goodid")+7;
            int disk = Math.abs((row.indexOf("\t",goodid_of) != -1 ? row.substring(goodid_of,row.indexOf("\t",goodid_of)) :
                    row.substring(goodid_of)).hashCode()%3);
            switch (disk) {
                case 0:
                    orderQueues[0].offer(row,60,TimeUnit.SECONDS);
                    break;
                case 1:
                    orderQueues[1].offer(row,60,TimeUnit.SECONDS);
                    break;
                case 2:
                    orderQueues[2].offer(row,60,TimeUnit.SECONDS);
                    break;
            }
        }

        @Override
        void handle(LinkedBlockingQueue<String> queue, Collection<String> files, final int keyNum,
                    final CountDownLatch latch) {
            new Thread(new FileHandler(keyNum,files,latch,this)).start();
        }
    }

    private class FileHandler implements Runnable {
        int keyNum;
        Collection<String> files;
        CountDownLatch latch;
        DataFileHandler handler;

        public FileHandler(int keyNum, Collection<String> files,CountDownLatch latch, DataFileHandler handler) {
            this.keyNum = keyNum;
            this.files = files;
            this.latch = latch;
            this.handler = handler;
        }
        @Override
        public void run() {
            for (String file : files) {
                BufferedReader bfr = null;
                try {
                    bfr = Utils.createReader(file);
                    String line = bfr.readLine();
                    while (line != null) {
                        handler.emit(line);
                        line = bfr.readLine();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (bfr!=null){
                        try {
                            bfr.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    System.out.println("original data: " + file+" complete. now time: "+(System.currentTimeMillis() -start ));
                    latch.countDown();
                }
            }
        }
    }
}

