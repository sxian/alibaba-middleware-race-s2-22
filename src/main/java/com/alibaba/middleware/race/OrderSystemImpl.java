package com.alibaba.middleware.race;

import com.alibaba.middleware.race.db.BuyerTable;
import com.alibaba.middleware.race.db.GoodsTable;
import com.alibaba.middleware.race.db.OrderTable;
import com.alibaba.middleware.race.process.FileProcessor;
import com.alibaba.middleware.race.process.IndexProcessor;
import com.alibaba.middleware.race.util.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * 订单系统的demo实现，订单数据全部存放在内存中，用简单的方式实现数据存储和查询功能
 *
 * @author wangxiang@alibaba-inc.com
 *
 */
public class OrderSystemImpl implements OrderSystem {

    private static String booleanTrueValue = "true";
    private static String booleanFalseValue = "false";

    // todo 优化线程数量，减少上下文切换开销
    private ExecutorService queryThreads = Executors.newFixedThreadPool(20);
    private ExecutorService constructThreads;

    public static final ArrayList<LinkedBlockingQueue<Row>> orderQueues = new ArrayList();
    public static final ArrayList<LinkedBlockingQueue<Row>> buyerQueues = new ArrayList();
    public static final ArrayList<LinkedBlockingQueue<Row>> goodsQueues = new ArrayList();

    // todo 确认一下查询的时候是多个线程持有一个OrderSystemImpl对象查询还是一个线程一个
    public FileProcessor fileProcessor;

    public static int orderQueueNum;
    public static int buyerQueueNum;
    public static int goodsQueueNum;

    IndexProcessor indexProcessor;
    private OrderTable orderTable;
    private BuyerTable buyerTable;
    private GoodsTable goodsTable;


    public OrderSystemImpl() {
        fileProcessor = new FileProcessor();
//        orderTree = fileProcessor.orderTree;
//        buyerTree = fileProcessor.buyerTree;
//        goodsTree = fileProcessor.goodsTree;
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

            if (queryingKeys.size()==0)
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

    private void initQueues() {
        for (int i = 0;i<orderQueueNum;i++) {
            orderQueues.add(new LinkedBlockingQueue<Row>());
        }
        for (int i = 0;i<buyerQueueNum;i++) {
            buyerQueues.add(new LinkedBlockingQueue<Row>());
        }
        for (int i = 0;i<goodsQueueNum;i++) {
            goodsQueues.add(new LinkedBlockingQueue<Row>());
        }
    }

    private void sendEndMsg() {
        for (LinkedBlockingQueue<Row> linkedBlockingQueue : orderQueues) {
            linkedBlockingQueue.offer(new Row());
        }
        for (LinkedBlockingQueue<Row> linkedBlockingQueue : buyerQueues) {
            linkedBlockingQueue.offer(new Row());
        }
        for (LinkedBlockingQueue<Row> linkedBlockingQueue : goodsQueues) {
            linkedBlockingQueue.offer(new Row());
        }
    }

    public void construct(Collection<String> orderFiles,
                          Collection<String> buyerFiles, Collection<String> goodFiles,
                          Collection<String> storeFolders) throws IOException, InterruptedException {
        int modNum = RaceConfig.CONSTRUCT_MOD_NUM;
        orderQueueNum = orderFiles.size()/modNum+1;
        buyerQueueNum = buyerFiles.size()/modNum+1;
        goodsQueueNum = goodFiles.size()/modNum+1;
        initQueues();

        fileProcessor = new FileProcessor();
        indexProcessor = new IndexProcessor();
        fileProcessor.init(storeFolders,indexProcessor);
        // 一个队列对应一个线程
        constructThreads = Executors.newFixedThreadPool(orderQueueNum+buyerQueueNum+goodsQueueNum);

        // 设置latch数目，确保所有数据都处理完
        final CountDownLatch latch = new CountDownLatch(orderFiles.size()+buyerFiles.size()+goodFiles.size());
        new DataFileHandler() {
            @Override
            void handleRow(Row row) throws InterruptedException {
                int index = Math.abs(row.get("goodid").rawValue.hashCode())%orderQueueNum;
                orderQueues.get(index).offer(row,60,TimeUnit.SECONDS);
            }
        }.handle(orderFiles, latch);

        new DataFileHandler() {
            @Override
            void handleRow(Row row) throws InterruptedException {
                int index = Math.abs(row.get("buyerid").rawValue.hashCode())%buyerQueueNum;
                buyerQueues.get(index).offer(row,60,TimeUnit.SECONDS);
            }
        }.handle(buyerFiles, latch);

        new DataFileHandler() {
            @Override
            void handleRow(Row row) throws InterruptedException {
                int index = Math.abs(row.get("goodid").rawValue.hashCode())%goodsQueueNum;
                goodsQueues.get(index).offer(row,60,TimeUnit.SECONDS);
            }
        }.handle(goodFiles, latch);

        latch.await(); // 等待处理完所有文件
        sendEndMsg(); // 发送结束信号
        fileProcessor.waitOver(); // 等待队列处理完毕
        constructThreads.shutdown();  // 销毁construct线程池
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

    private abstract class DataFileHandler {
        abstract void handleRow(Row row) throws InterruptedException;

        void handle(Collection<String> files, final CountDownLatch latch) {
            for (final String file : files) {
                constructThreads.execute(new Runnable() {
                    @Override
                    public void run() {
                        BufferedReader bfr = null;
                        try {
                            bfr = Utils.createReader(file);
                            String line = bfr.readLine();
                            while (line != null) {
                                Row kvMap = createRow(line);
                                handleRow(kvMap);
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
                });
            }
        }
    }

    @Override
    public Result queryOrder(long orderId, Collection<String> keys) {
        Row orderRow = orderTable.selectRowById(String.valueOf(orderId));
        if (orderRow  == null)
            return null;
        Row buyerRow = buyerTable.selectRowById(orderRow.get("buyerid").valueAsString());
        Row goodsRow = buyerTable.selectRowById(orderRow.get("goodid").valueAsString());
        return ResultImpl.createResultRow(orderRow, buyerRow, goodsRow, new HashSet<String>(keys));
    }

    @Override
    public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime, String buyerid) {
        ArrayList<Result> results = new ArrayList<>();
        for (String orderId : orderTable.selectOrderIDByBuyerID(buyerid,startTime,endTime)) {
            results.add(queryOrder(Long.valueOf(orderId),null)); // todo 按照createtime大到小排列
        }
        return results.iterator();
    }

    @Override
    public Iterator<Result> queryOrdersBySaler(String salerid, String goodid, Collection<String> keys) {
        ArrayList<Result> results = new ArrayList<>();
        for (String orderId : orderTable.selectOrderIDByGoodsID(goodid)) {
            results.add(queryOrder(Long.valueOf(orderId),keys));
        }
        return results.iterator();
    }

    @Override
    public KeyValue sumOrdersByGood(String goodid, String key) {
        ArrayList<Result> results = new ArrayList<>();
        List<String> list =  orderTable.selectOrderIDByGoodsID(goodid);
        double sumDouble = 0;
        long sumLong = 0;
        boolean existKey = false;
        boolean existDouble = false;
        boolean existStr = false;
        if (list == null)
            return null;

        for (String orderId : list) {
            Row orderRow = orderTable.selectRowById(orderId); // todo 肯定不为空
            KV kv = orderRow.get(key);
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
                    } catch (TypeException e1) {
                        e1.printStackTrace(); // 通过测试后可以删掉
                    }
                }
                e.printStackTrace();
                existStr = true;
                break;
            }
        }

        if (existDouble) {
            return new KV(key,String.valueOf(sumDouble));
        }
        return (!existKey || existStr) ? null : new KV(key,String.valueOf(sumLong));
    }
}
