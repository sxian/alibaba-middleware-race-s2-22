package com.alibaba.middleware.race;

import com.alibaba.middleware.race.process.FileProcessor;

import java.io.*;
import java.nio.channels.Channel;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 订单系统的demo实现，订单数据全部存放在内存中，用简单的方式实现数据存储和查询功能
 *
 * @author wangxiang@alibaba-inc.com
 *
 */
public class OrderSystemImpl implements OrderSystem {

    private static String booleanTrueValue = "true";
    private static String booleanFalseValue = "false";

    private ExecutorService queryThreads = Executors.newFixedThreadPool(20);
    private ExecutorService constructThreads = Executors.newCachedThreadPool();

    public static final LinkedBlockingQueue<Row> orderQueue = new LinkedBlockingQueue<>(100000);
    public static final LinkedBlockingQueue<Row> buyerQueue = new LinkedBlockingQueue<>(100000);
    public static final LinkedBlockingQueue<Row> goodsQueue = new LinkedBlockingQueue<>(100000);


    public OrderSystemImpl() {
//        orderQueue.m
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

    private BufferedReader createReader(String file) throws FileNotFoundException {
        return new BufferedReader(new FileReader(file));
    }

    public void construct(Collection<String> orderFiles,
                          Collection<String> buyerFiles, Collection<String> goodFiles,
                          Collection<String> storeFolders) throws IOException, InterruptedException {
        final Semaphore semaphore = new Semaphore(0);
        final AtomicInteger threadCount = new AtomicInteger(0);
        FileProcessor fileProcessor = new FileProcessor();
        fileProcessor.init(storeFolders);

        new DataFileHandler() {
            @Override
            void handleRow(Row row) {
                orderQueue.offer(row);
            }
        }.handle(orderFiles, semaphore, threadCount);

        new DataFileHandler() {
            @Override
            void handleRow(Row row) {
                buyerQueue.offer(row);
            }
        }.handle(buyerFiles, semaphore, threadCount);

        new DataFileHandler() {
            @Override
            void handleRow(Row row) {
                goodsQueue.offer(row);
            }
        }.handle(goodFiles, semaphore, threadCount);
//        new
        semaphore.acquire(threadCount.get());
        System.out.println();
    }

    private Row createRow(String line) {
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

    private abstract class DataFileHandler {
        abstract void handleRow(Row row);

        void handle(Collection<String> files, final Semaphore semaphore, final AtomicInteger threadCount) throws IOException {
            for (final String file : files) {
                constructThreads.execute(new Runnable() {
                    @Override
                    public void run() {
                        threadCount.addAndGet(1);
                        BufferedReader bfr = null;
                        try {
                            bfr = createReader(file);
                            String line = bfr.readLine();
                            while (line != null) {
                                Row kvMap = createRow(line);
                                handleRow(kvMap);
                                line = bfr.readLine();
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        } finally {
                            if (bfr!=null){
                                try {
                                    bfr.close();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                            semaphore.release();
                        }
                    }
                });
            }
        }
    }

    @Override
    public Result queryOrder(long orderId, Collection<String> keys) {
        return null;
    }

    @Override
    public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime, String buyerid) {
        return null;
    }

    @Override
    public Iterator<Result> queryOrdersBySaler(String salerid, String goodid, Collection<String> keys) {
        return null;
    }

    @Override
    public KeyValue sumOrdersByGood(String goodid, String key) {
        return null;
    }
}
