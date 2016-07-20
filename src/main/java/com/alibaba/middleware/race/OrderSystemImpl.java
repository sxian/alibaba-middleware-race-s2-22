package com.alibaba.middleware.race;

import java.io.*;
import java.nio.channels.Channel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * 订单系统的demo实现，订单数据全部存放在内存中，用简单的方式实现数据存储和查询功能
 *
 * @author wangxiang@alibaba-inc.com
 *
 */
public class OrderSystemImpl implements OrderSystem {

    static private String booleanTrueValue = "true";
    static private String booleanFalseValue = "false";

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

    static private class ComparableKeys implements Comparable<ComparableKeys> {
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

    public OrderSystemImpl() {

    }

    public static void main(String[] args) throws IOException,
            InterruptedException {

        // init order system
        List<String> orderFiles = new ArrayList<String>();
        List<String> buyerFiles = new ArrayList<String>();
        ;
        List<String> goodFiles = new ArrayList<String>();
        List<String> storeFolders = new ArrayList<String>();

        orderFiles.add("order_records.txt");
        buyerFiles.add("buyer_records.txt");
        goodFiles.add("good_records.txt");
        storeFolders.add("./");

        OrderSystem os = new OrderSystemImpl();
        os.construct(orderFiles, buyerFiles, goodFiles, storeFolders);

        // 用例
        long orderid = 2982388;
        System.out.println("\n查询订单号为" + orderid + "的订单");
        System.out.println(os.queryOrder(orderid, null));

        System.out.println("\n查询订单号为" + orderid + "的订单，查询的keys为空，返回订单，但没有kv数据");
        System.out.println(os.queryOrder(orderid, new ArrayList<String>()));

        System.out.println("\n查询订单号为" + orderid
                + "的订单的contactphone, buyerid, foo, done, price字段");
        List<String> queryingKeys = new ArrayList<String>();
        queryingKeys.add("contactphone");
        queryingKeys.add("buyerid");
        queryingKeys.add("foo");
        queryingKeys.add("done");
        queryingKeys.add("price");
        Result result = os.queryOrder(orderid, queryingKeys);
        System.out.println(result);
        System.out.println("\n查询订单号不存在的订单");
        result = os.queryOrder(1111, queryingKeys);
        if (result == null) {
            System.out.println(1111 + " order not exist");
        }

        String buyerid = "tb_a99a7956-974d-459f-bb09-b7df63ed3b80";
        long startTime = 1471025622;
        long endTime = 1471219509;
        System.out.println("\n查询买家ID为" + buyerid + "的一定时间范围内的订单");
        Iterator<Result> it = os.queryOrdersByBuyer(startTime, endTime, buyerid);
        while (it.hasNext()) {
            System.out.println(it.next());
        }

        String goodid = "good_842195f8-ab1a-4b09-a65f-d07bdfd8f8ff";
        String salerid = "almm_47766ea0-b8c0-4616-b3c8-35bc4433af13";
        System.out.println("\n查询商品id为" + goodid + "，商家id为" + salerid + "的订单");
        it = os.queryOrdersBySaler(salerid, goodid, new ArrayList<String>());
        while (it.hasNext()) {
            System.out.println(it.next());
        }

        goodid = "good_d191eeeb-fed1-4334-9c77-3ee6d6d66aff";
        String attr = "app_order_33_0";
        System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
        System.out.println(os.sumOrdersByGood(goodid, attr));

        attr = "done";
        System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
        KeyValue sum = os.sumOrdersByGood(goodid, attr);
        if (sum == null) {
            System.out.println("由于该字段是布尔类型，返回值是null");
        }

        attr = "foo";
        System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
        sum = os.sumOrdersByGood(goodid, attr);
        if (sum == null) {
            System.out.println("由于该字段不存在，返回值是null");
        }
    }

    private BufferedReader createReader(String file) throws FileNotFoundException {
        return new BufferedReader(new FileReader(file));
    }

    private Row createKVMapFromLine(String line) {
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

        void handle(Collection<String> files) throws IOException {
            for (String file : files) {
                BufferedReader bfr = createReader(file);
                try {
                    String line = bfr.readLine();
                    while (line != null) {
                        Row kvMap = createKVMapFromLine(line);
                        handleRow(kvMap);
                        line = bfr.readLine();
                    }
                } finally {
                    bfr.close();
                }
            }
        }
    }

    public void construct(Collection<String> orderFiles,
                          Collection<String> buyerFiles, Collection<String> goodFiles,
                          Collection<String> storeFolders) throws IOException, InterruptedException {


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
