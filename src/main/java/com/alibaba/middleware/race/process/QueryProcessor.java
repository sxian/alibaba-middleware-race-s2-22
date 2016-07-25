package com.alibaba.middleware.race.process;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.cache.LRUCache;
import com.alibaba.middleware.race.datastruct.RecordIndex;

import java.io.RandomAccessFile;
import java.util.*;

/**
 * Created by sxian.wang on 2016/7/21.
 */
public class QueryProcessor {
    // 有没有必要把这里设置为static -> todo 同一个RandomAccessFile 读的时候能共用吗
    private static final HashMap<String, RandomAccessFile> randomAccessFileHashMap = new HashMap<>();

    private static final LRUCache<String,RecordIndex> orderIndexCache = new LRUCache<>(100000); // orderid 5e 这个地方索引缓存
    private static final LRUCache<String,RecordIndex> buyerIndexCache = new LRUCache<>(8000000); // buyerid 800w 看下内存占用
    private static final LRUCache<String,RecordIndex> goodsIndexCache = new LRUCache<>(4000000); // goodid 400w

    // todo 不同文件的B+树索引能否合并
    public static HashMap<String, TreeMap<Long,Long[]>> filesIndex = new HashMap<>();
    public static HashMap<String, Long[]> filesIndexKey = new HashMap<>();

    // 把所有的索引使用的B+树的前提是B+树中有所有的信息节点
    public static TreeMap<String,Long[]>  buyerOrderFilesIndex = new TreeMap<>();
    public static ArrayList<String> buyerOrderFilesIndexKey = new ArrayList<>();

    public static TreeMap<String,Long[]>  goodsOrderFilesIndex = new TreeMap<>();
    public static ArrayList<String> goodsOrderFilesIndexKey = new ArrayList<>();

    public static void addIndexCache(RecordIndex index, int flag) {
        if (flag == 0) {
            buyerIndexCache.put(index.key,index);
        } else {
            goodsIndexCache.put(index.key,index);
        }
    }

    public static String queryOrder(String id) {
        RecordIndex indexCache = orderIndexCache.get(id);  // todo orderid的索引缓存是有问题的，和rawdata重叠了
        if (indexCache == null) {
            String path = RaceConfig.ORDER_SOTRED_STORE_PATH+"oS"+(id.hashCode()%RaceConfig.ORDER_FILE_SIZE);
            long orderid = -1;
            try {

                orderid = Long.valueOf(id);
            } catch (Exception e) {
                int i = 0;
            }

            Long[] idKeys = filesIndexKey.get(path);
            if (idKeys==null) {
                idKeys = (Long[]) filesIndex.get(path).keySet().toArray();
                filesIndexKey.put(path, idKeys);
            }

            long key;
            if (orderid<idKeys[0]) {
                key = idKeys[0];
            } else if(orderid>idKeys[idKeys.length-1]) {
                key = idKeys[idKeys.length-1];
            } else {
                key = binarySearch(idKeys,orderid);
            }
            Long[] pos = filesIndex.get(path).get(key);
            return queryRowByBPT(path, orderid, pos[0],Integer.valueOf(String.valueOf(pos[1])));
        }
        return queryByIndex(indexCache);
    }

    public static List<String> queryOrderidsByBuyerid(String buyerid, long start, long end) {
        String path = RaceConfig.STORE_PATH+"buyerid_create_order";
        return queryRangeOrder(buyerOrderFilesIndex,buyerOrderFilesIndexKey,path,buyerid,start,end);
    }

    public static List<String> queryOrderidsByGoodsid(String goodid) {
        String path = RaceConfig.STORE_PATH+"goodid_orderid";
        return queryRangeOrder(goodsOrderFilesIndex,goodsOrderFilesIndexKey,path,goodid,null,null);
    }

    public static List<String> queryRangeOrder(TreeMap<String,Long[]> map, ArrayList<String> list, String path,
                                               String id, Long start, Long end){
//        String key;
//        if (id.compareTo(list.get(0)) < 0) {
//            key = list.get(0);
//        } else if(id.compareTo(list.get(list.size()-1)) > 0) {
//            key = list.get(list.size()-1);
//        } else {
//            key = binarySearchString(list, id);
//        }
        Long[] pos = map.get(id);
        if (pos == null) {
            return new ArrayList<>(); // todo 索引全加载到内存可以这样做，如果分开的话return null的操作不应该在这里
//            throw new RuntimeException("range query order id error");
        }

        String result = queryRowStringByBPT(path, id, pos[0],Integer.valueOf(String.valueOf(pos[1])));
        if (result == null) {
            return null;
        }
        List<String> orderList = Arrays.asList(result.split(" "));
        List<String> newList = new ArrayList<>();
        if (start != null) {
            for (String str : orderList) {  // orderList 是无序的
                String[] kv = str.split(",");
                long time = 0;
                try {
                    time = Long.valueOf(kv[1]);
                } catch (Exception e) {
                    int i = 1;
                }
                if (time>= start && time < end){
                    // todo 在处理文件数据的时候换下位置
                    newList.add(kv[1]+","+kv[0]); // 因为要按时间排序，所以返回有序的结果 -> 不能光添加订单，否则会按照订单排序
                }
            }
            Collections.sort(newList, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return -(o1.compareTo(o2));
                }
            });
            return newList;
        }
        Collections.sort(newList);  // 光有订单id, 按照订单id升序排列(从小到大)
        return orderList;
    }

    // buyer和goods的索引全在内存里面。
    public static String queryBuyer(String id) {
        RecordIndex indexCache = buyerIndexCache.get(id);
        if (indexCache == null) {
            throw new RuntimeException("buyerid index error");
        }
        return queryByIndex(indexCache);
    }

    public static String queryGoods(String id) {
        RecordIndex indexCache = goodsIndexCache.get(id);
        if (indexCache == null) {
            throw new RuntimeException("goodsid index error");
        }
        return queryByIndex(indexCache);
    }

    private static String queryByIndex(RecordIndex index) {
        return queryRow(index.filePath, index.position, index.length);
    }

    private static String queryRow(String file, long pos, int length) {
        byte[] bytes = new byte[length];
        try {
            RandomAccessFile raf = randomAccessFileHashMap.get(file);
            if (raf == null) {
                raf = new RandomAccessFile(file,"r");
                randomAccessFileHashMap.put(file, raf);
            }
            raf.seek(pos);
            raf.read(bytes);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new String(bytes);
    }

    // todo 可以用queryRowStringByBPT替代queryRowByBPT
    private static String queryRowByBPT(String file, long orderid, long pos, int length) {
        byte[] bytes = new byte[length];
        boolean findRow = false;
        int rowLen = 0;
        try {
            RandomAccessFile raf = randomAccessFileHashMap.get(file);
            if (raf == null) {
                raf = new RandomAccessFile(file,"r");
                randomAccessFileHashMap.put(file, raf);
            }
            while (!findRow) {
                raf.seek(pos);
                raf.read(bytes);
                String[] bIndexs = new String(bytes,0,length).split(" ");
                if (bIndexs[0].equals("0")) {
                    boolean findIndex = false;
                    String[] preIndexPos = bIndexs[1].split(",");
                    if (Long.valueOf(preIndexPos[0])>orderid) { // 第一个节点满足
                        pos = Long.valueOf(preIndexPos[1]);
                        length = Integer.valueOf(preIndexPos[2]);
                        continue;
                    }
                    if (!findIndex) {
                        for (int i = 1; i<bIndexs.length-1;i++) {
                            String[] indexPos = bIndexs[i+1].split(",");
                            if (i==bIndexs.length-2) {
                                if (orderid < Long.valueOf(preIndexPos[0])) {
                                    throw new RuntimeException("compare error");
                                }
                                pos = Long.valueOf(preIndexPos[1]);
                                length = Integer.valueOf(preIndexPos[2]);
                                break;
                            }

                            if (Long.valueOf(preIndexPos[0]) <= orderid && Long.valueOf(indexPos[0]) > orderid) {
                                pos = Long.valueOf(preIndexPos[1]);
                                length = Integer.valueOf(preIndexPos[2]);
                                break;
                            }
                            preIndexPos = indexPos;
                        }
                    }
                } else {
                    for (int i = 1; i<bIndexs.length;i++) {
                        if (bIndexs[i].equals("\n")) {  // todo 最终版把写到文件的\n全删了
                            continue;
                        }
                        String[] rowPos = bIndexs[i].split(",");
                        try {
                        if (Long.valueOf(rowPos[0])==orderid) {

                            raf.seek(Long.valueOf(rowPos[1]));
                            rowLen = Integer.valueOf(rowPos[2]);
                            if (rowLen > bytes.length) {
                                bytes = new byte[rowLen];
                            }
                            raf.read(bytes,0,rowLen);
                            findRow = true;
                            break;
                        }
                        } catch (Exception e) {
                            int as = 0;
                        }

                    }
                    break;
                }
                if (length > bytes.length) {
                    bytes = new byte[length];
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return findRow ? new String(bytes,0,rowLen) : null;
    }

    private static String queryRowStringByBPT(String file, String id, long pos, int length) {
        byte[] bytes = new byte[length];
        boolean findRow = false;
        int rowLen = 0;
        try {
            RandomAccessFile raf = randomAccessFileHashMap.get(file);
            if (raf == null) {
                raf = new RandomAccessFile(file,"r");
                randomAccessFileHashMap.put(file, raf);
            }
            while (!findRow) {
                raf.seek(pos);
                raf.read(bytes);
                String[] bIndexs = new String(bytes,0,length).split(" ");

                if (bIndexs[0].equals("0")) { // todo 在读取goodid_orderid的时候会超出范围
                    boolean findIndex = false;
                    String[] preIndexPos = bIndexs[1].split(",");
                    if (preIndexPos[0].compareTo(id)>0) { // 第一个节点满足
                        pos = Long.valueOf(preIndexPos[1]);
                        length = Integer.valueOf(preIndexPos[2]);
                        continue;
                    }
                    if (!findIndex) {
                        for (int i = 1; i<bIndexs.length-1;i++) {
                            String[] indexPos = bIndexs[i+1].split(",");
                            if (i==bIndexs.length-2) {
                                if (id.compareTo(preIndexPos[0]) < 0) {
                                    throw new RuntimeException("compare error");
                                }
                                pos = Long.valueOf(preIndexPos[1]);
                                length = Integer.valueOf(preIndexPos[2]);
                                break;
                            }

                            if (preIndexPos[0].compareTo(id) <= 0 && indexPos[0].compareTo(id) > 0 ) {
                                pos = Long.valueOf(preIndexPos[1]);
                                length = Integer.valueOf(preIndexPos[2]);
                                break;
                            }
                            preIndexPos = indexPos;
                        }
                    }
                } else if (bIndexs[0].equals("1")){
                    for (int i = 1; i<bIndexs.length;i++) {
                        String[] rowPos = bIndexs[i].split(",");
                        if (rowPos[0].equals(id)) {
                            raf.seek(Long.valueOf(rowPos[1]));
                            rowLen = Integer.valueOf(rowPos[2]);
                            if (rowLen > bytes.length) {
                                bytes = new byte[rowLen];
                            }
                            raf.read(bytes,0,rowLen);
                            findRow = true;
                            break;
                        }
                    }
                    break;
                } else {
                    return new String(bytes,0,length);
                }
                if (length > bytes.length) {
                    bytes = new byte[length];
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return findRow ? new String(bytes,0,rowLen) : null;
    }

    public static long binarySearch(Long[] data, long key) {
        int min, max, mid;
        min = 0;
        max = data.length - 1;
        while(!((max-min)== 1)){
            mid = (min + max) / 2;
            if(key < data[mid]){
                max = mid;  // 因为不清楚data[mid-1]是不是满足 key<data[mid-1],可能存在key>=data[min+1]的情况
            }else if (key > data[mid]){
                min = mid;  // 因为不清楚data[mid+1]是不是满足 key>=data[mid+1],可能存在data[min+1]>key的情况
            }else if(key == data[mid]){
                return data[mid];
            }
        }

        if (data[max] <= key) {
            return data[max];
        }
        return data[min];
    }

    private static String binarySearchString(ArrayList<String> data, String key) {
        int min, max, mid;
        min = 0;
        max = data.size() - 1;
        while(!((max-min)== 1)){
            mid = (min + max) / 2;
            if(key.compareTo(data.get(mid))<0){
                max = mid;  // 因为不清楚data[mid-1]是不是满足 key<data[mid-1],可能存在key>=data[min+1]的情况
            }else if (key.compareTo(data.get(mid))>0){
                min = mid;  // 因为不清楚data[mid+1]是不是满足 key>=data[mid+1],可能存在data[min+1]>key的情况
            }else if(key.equals(data.get(mid))){
                return data.get(mid);
            }
        }

        if (data.get(min).compareTo(key) <=0 ) {
            return data.get(min);
        }
        return data.get(min);
    }
}
