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
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    public LinkedBlockingQueue<String[][]>[] orderQueues = new LinkedBlockingQueue[3];
    public LinkedBlockingQueue<String[][]> buyerQueue = new LinkedBlockingQueue<>(150000);
    public LinkedBlockingQueue<String[][]> goodsQueue = new LinkedBlockingQueue<>(150000);

    private FileProcessor fileProcessor;
    private IndexProcessor indexProcessor;

    private OrderTable orderTable;
    private BuyerTable buyerTable;
    private GoodsTable goodsTable;
    private CountDownLatch tmp_latch = new CountDownLatch(1);

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

    private void sendEndMsg(LinkedBlockingQueue<String[][]>[] queues) {
        for (LinkedBlockingQueue<String[][]> queue : queues) {
            queue.offer(new String[0][0]);
        }
    }

    public void construct(Collection<String> orderFiles,
                          Collection<String> buyerFiles, Collection<String> goodFiles,
                          Collection<String> storeFolders) throws IOException, InterruptedException {
        ArrayList<String> disk1;
        ArrayList<String> disk2 = new ArrayList<>();
        ArrayList<String> disk3 = new ArrayList<>();

        if (RaceConfig.ONLINE) {
            RaceConfig.ORDER_FILE_SIZE = 41; // todo 要分到3个磁盘，所以实际文件数量是三倍
            RaceConfig.BUYER_FILE_SIZE = 10;
            RaceConfig.GOODS_FILE_SIZE = 10;
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
            System.out.println("*** order file num: "+orderFiles.size()+" ***");
            disk1 = new ArrayList<>();
            for (String file : orderFiles) {
                if (file.startsWith("/disk1")) {
                    disk1.add(file);
                } else if (file.startsWith("/disk2")) {
                    disk2.add(file);
                } else {
                    disk3.add(file);
                }
                System.out.println(file);
            }
            System.out.println("*** buyer file num: "+buyerFiles.size()+" ***");
            for (String file : buyerFiles) {
                System.out.println(file);
            }
            System.out.println("*** goods file num: "+goodFiles.size()+" ***");
            for (String file : goodFiles) {
                System.out.println(file);
            }
        } else {
            disk1 = new ArrayList<>(orderFiles);
        }

        for (int i = 0;i<3;i++) { // todo 参数优化
            orderQueues[i] = new LinkedBlockingQueue<>(100000);
        }

        // 一个队列对应一个线程

        // 设置latch数目，确保所有数据都处理完
        final CountDownLatch orderLatch = new CountDownLatch(orderFiles.size());
        final CountDownLatch buyerLatch = new CountDownLatch(buyerFiles.size());
        final CountDownLatch goodsLatch = new CountDownLatch(goodFiles.size());

        long start = System.currentTimeMillis();

        // 5个读取数据的线程 todo 加回调，读一个磁盘，写一个磁盘，保证同时一个磁盘只有读或写，还得保持并发，不能让cpu闲着
        new DataFileHandler().handle(orderQueues[0], disk1, 4, orderLatch,
                "(orderid|buyerid|goodid|createtime):([\\w|-]+)");
        new DataFileHandler().handle(orderQueues[1], disk2, 4, orderLatch,
                "(orderid|buyerid|goodid|createtime):([\\w|-]+)");
        new DataFileHandler().handle(orderQueues[2], disk3, 4, orderLatch,
                "(orderid|buyerid|goodid|createtime):([\\w|-]+)");

        new DataFileHandler().handle(buyerQueue, buyerFiles,1, buyerLatch, "(buyerid):([\\w|-]+)");

        new DataFileHandler().handle(goodsQueue, goodFiles, 1, goodsLatch, "(goodid):([\\w|-]+)");

        indexProcessor = new IndexProcessor(start);
        fileProcessor.init(start, indexProcessor);
        indexProcessor = null;

        buyerLatch.await();
        buyerQueue.offer(new String[0][0]);
        buyerQueue = null;
        System.out.println("process buyer data, now time: " + (System.currentTimeMillis() - start));
        goodsLatch.await();
        goodsQueue.offer(new String[0][0]);
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
        String orderRowStr = orderTable.selectRowById(String.valueOf(orderId));
        // todo 官方的借口demo改了一下，方便build result 看着改下
        if (orderRowStr  == null)
            return null;
        Row orderRow = createRow(orderRowStr); // todo 一直build真特么费时间 -> 判断join不join很重要
        Row buyerRow = createRow(buyerTable.selectRowById(orderRow.get("buyerid").valueAsString()));
        Row goodsRow = createRow(goodsTable.selectRowById(orderRow.get("goodid").valueAsString()));
        if (keys == null) {
            return ResultImpl.createResultRow(orderRow, buyerRow, goodsRow, null);
        }
        return ResultImpl.createResultRow(orderRow, buyerRow, goodsRow, new HashSet<>(keys));
    }

    @Override
    public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime, String buyerid) {
        try {
            tmp_latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        ArrayList<Result> results = new ArrayList<>();

        for (String orderId : orderTable.selectOrderIDByBuyerID(buyerid,startTime,endTime)) {
            results.add(queryOrder(Long.valueOf(orderId.split(",")[1]),null));
        }
        return results.iterator();
    }

    @Override
    public Iterator<Result> queryOrdersBySaler(String salerid, String goodid, Collection<String> keys) {
        try {
            tmp_latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        ArrayList<Result> results = new ArrayList<>();
        for (String orderId : orderTable.selectOrderIDByGoodsID(goodid)) {
            results.add(queryOrder(Long.valueOf(orderId),keys));
        }
        return results.iterator();
    }

    @Override
    public KeyValue sumOrdersByGood(String goodid, String key) {
        try {
            tmp_latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        List<String> list =  orderTable.selectOrderIDByGoodsID(goodid);
        List<String> _list = new ArrayList<>();
        _list.add(key);
        double sumDouble = 0;
        long sumLong = 0;
        boolean existKey = false;
        boolean existDouble = false;
        boolean existStr = false;
        if (list == null)
            return null;

        for (String orderId : list) {
            Result result = queryOrder(Long.valueOf(orderId),_list); // todo 肯定不为空 所有字段都是join后的
            KV kv = null;

//            if (result != null) {
            kv = (KV) result.get(key);
//            } else {
//                result = queryOrder(Long.valueOf(orderId),_list);
//                continue;
//            }
            if (kv == null)
                continue;
            if (!existKey) {
                existKey = true;
            }
            try {
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
        } // todo 大量的未包含元素查找
        return (!existKey || existStr) ? null : new KV(key,String.valueOf(sumLong));
    }

    private class DataFileHandler {
        LinkedBlockingQueue<String[][]> queue;
        void handle(LinkedBlockingQueue<String[][]> queue, Collection<String> files, final int keyNum,
                    final CountDownLatch latch, final String regx) {
            this.queue = queue;
            new Thread(new FileHandler(keyNum,files,regx,latch,this)).start();
        }
    }

    private class FileHandler implements Runnable {
        int keyNum;
        Collection<String> files;
        String regx;
        CountDownLatch latch;
        DataFileHandler handler;

        public FileHandler(int keyNum, Collection<String> files, String regx, CountDownLatch latch,
                           DataFileHandler handler) {
            this.keyNum = keyNum;
            this.files = files;
            this.regx = regx;
            this.latch = latch;
            this.handler = handler;
        }
        @Override
        public void run() {
            Pattern pattern = Pattern.compile(regx);
            for (String file : files) {
                BufferedReader bfr = null;
                try {
                    bfr = Utils.createReader(file);
                    String line = bfr.readLine();
                    while (line != null) {
                        String[][] strings = new String[keyNum+1][2];
                        Matcher matcher = pattern.matcher(line);
                        strings[0][0] = line;
                        int i = 1;
//                        line.indexOf("orderid");
//                        line.split(regx);
                        while (matcher.find()) { // todo 所有的字符串切割操作,全改为indexof何substring, 并且和正则比较下, 去掉所有的split
                                                 // order 前四位分别是 orderid, createtime, buyerid, goodid,
                                                 // buyer和goods第一个是id
                            strings[i][0] = matcher.group(1);
                            strings[i][1] = matcher.group(2);
                            i++;
                        }
                        handler.queue.offer(strings,60,TimeUnit.SECONDS);
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
                    latch.countDown();
                }
            }
        }
    }
}

